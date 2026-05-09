package dev.feddi.api.usage;

import dev.feddi.api.usage.v1.InputUsageCoordinateKind;
import graphql.execution.CoercedVariables;
import graphql.language.AstPrinter;
import graphql.language.AstSignature;
import graphql.language.Argument;
import graphql.language.ArrayValue;
import graphql.language.Document;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.ObjectValue;
import graphql.language.OperationDefinition;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.language.Value;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeUtil;
import graphql.schema.GraphQLUnmodifiedType;
import graphql.schema.GraphQLUnionType;
import org.jspecify.annotations.Nullable;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class ApiUsageDocumentAnalyzer {

    ProcessedUsage analyze(ApiUsageInvocation invocation) {
        var operation = findOperation(invocation.document(), invocation.operationName());
        var signatureDocument = new AstSignature().signatureWithInput(
                invocation.document(),
                invocation.operationName(),
                invocation.schema(),
                CoercedVariables.of(invocation.variables())
        );
        var signatureOperation = findOperation(signatureDocument, invocation.operationName());
        var canonicalDocument = AstPrinter.printAstCompact(signatureDocument);
        var usageCoordinates = extractUsageCoordinates(
                signatureOperation,
                signatureDocument,
                invocation.schema()
        );

        return new ProcessedUsage(
                operation.getName(),
                operation.getOperation().name(),
                canonicalDocument,
                usageCoordinates.fieldCoordinates(),
                usageCoordinates.inputUsageCoordinates()
        );
    }

    private static OperationDefinition findOperation(Document document, @Nullable String operationName) {
        var operations = document.getDefinitionsOfType(OperationDefinition.class);
        if (operationName != null && !operationName.isBlank()) {
            return operations.stream()
                    .filter(operation -> operationName.equals(operation.getName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("GraphQL operation not found: " + operationName));
        }
        if (operations.size() == 1) {
            return operations.getFirst();
        }
        throw new IllegalArgumentException("operationName is required when the document contains multiple operations");
    }

    private static UsageCoordinates extractUsageCoordinates(
            OperationDefinition operation,
            Document document,
            GraphQLSchema schema
    ) {
        Set<String> fieldCoordinates = new LinkedHashSet<>();
        Set<InputUsageCoordinate> inputUsageCoordinates = new LinkedHashSet<>();
        var fragments = document.getDefinitionsOfType(FragmentDefinition.class).stream()
                .collect(Collectors.toMap(FragmentDefinition::getName, fragment -> fragment));

        GraphQLObjectType rootType = switch (operation.getOperation()) {
            case QUERY -> schema.getQueryType();
            case MUTATION -> schema.getMutationType();
            case SUBSCRIPTION -> schema.getSubscriptionType();
        };
        if (rootType == null) {
            return new UsageCoordinates(List.of(), List.of());
        }

        collectFields(
                rootType,
                operation.getSelectionSet(),
                fieldCoordinates,
                inputUsageCoordinates,
                fragments,
                schema,
                new HashSet<>()
        );
        return new UsageCoordinates(List.copyOf(fieldCoordinates), List.copyOf(inputUsageCoordinates));
    }

    private static void collectFields(
            GraphQLUnmodifiedType parentType,
            SelectionSet selectionSet,
            Set<String> fieldCoordinates,
            Set<InputUsageCoordinate> inputUsageCoordinates,
            Map<String, FragmentDefinition> fragments,
            GraphQLSchema schema,
            Set<String> activeFragments
    ) {
        if (selectionSet == null) {
            return;
        }

        for (Selection<?> selection : selectionSet.getSelections()) {
            if (selection instanceof Field field) {
                collectField(
                        parentType,
                        field,
                        fieldCoordinates,
                        inputUsageCoordinates,
                        fragments,
                        schema,
                        activeFragments
                );
            } else if (selection instanceof FragmentSpread spread) {
                collectFragment(
                        spread,
                        fieldCoordinates,
                        inputUsageCoordinates,
                        fragments,
                        schema,
                        activeFragments
                );
            } else if (selection instanceof InlineFragment inlineFragment) {
                collectInlineFragment(
                        parentType,
                        inlineFragment,
                        fieldCoordinates,
                        inputUsageCoordinates,
                        fragments,
                        schema,
                        activeFragments
                );
            }
        }
    }

    private static void collectField(
            GraphQLUnmodifiedType parentType,
            Field field,
            Set<String> fieldCoordinates,
            Set<InputUsageCoordinate> inputUsageCoordinates,
            Map<String, FragmentDefinition> fragments,
            GraphQLSchema schema,
            Set<String> activeFragments
    ) {
        String fieldName = field.getName();
        if (fieldName.startsWith("__")) {
            return;
        }

        if (!(parentType instanceof GraphQLFieldsContainer fieldsContainer)) {
            return;
        }

        GraphQLFieldDefinition fieldDefinition = fieldsContainer.getFieldDefinition(fieldName);
        if (fieldDefinition != null) {
            String fieldCoordinate = fieldsContainer.getName() + "." + fieldName;
            fieldCoordinates.add(fieldCoordinate);
            collectFieldArguments(
                    fieldCoordinate,
                    field,
                    fieldDefinition,
                    inputUsageCoordinates
            );
        }

        if (fieldDefinition == null || field.getSelectionSet() == null) {
            return;
        }

        GraphQLUnmodifiedType unwrapped = GraphQLTypeUtil.unwrapAll(fieldDefinition.getType());
        if (isSelectableParentType(unwrapped)) {
            collectFields(
                    unwrapped,
                    field.getSelectionSet(),
                    fieldCoordinates,
                    inputUsageCoordinates,
                    fragments,
                    schema,
                    activeFragments
            );
        }
    }

    private static void collectFragment(
            FragmentSpread spread,
            Set<String> fieldCoordinates,
            Set<InputUsageCoordinate> inputUsageCoordinates,
            Map<String, FragmentDefinition> fragments,
            GraphQLSchema schema,
            Set<String> activeFragments
    ) {
        String fragmentName = spread.getName();
        FragmentDefinition fragment = fragments.get(fragmentName);
        if (fragment == null) {
            return;
        }
        if (!activeFragments.add(fragmentName)) {
            return;
        }

        try {
            GraphQLType type = schema.getType(fragment.getTypeCondition().getName());
            if (type instanceof GraphQLUnmodifiedType unmodifiedType && isSelectableParentType(unmodifiedType)) {
                collectFields(
                        unmodifiedType,
                        fragment.getSelectionSet(),
                        fieldCoordinates,
                        inputUsageCoordinates,
                        fragments,
                        schema,
                        activeFragments
                );
            }
        } finally {
            activeFragments.remove(fragmentName);
        }
    }

    private static void collectInlineFragment(
            GraphQLUnmodifiedType parentType,
            InlineFragment inlineFragment,
            Set<String> fieldCoordinates,
            Set<InputUsageCoordinate> inputUsageCoordinates,
            Map<String, FragmentDefinition> fragments,
            GraphQLSchema schema,
            Set<String> activeFragments
    ) {
        if (inlineFragment.getTypeCondition() == null) {
            collectFields(
                    parentType,
                    inlineFragment.getSelectionSet(),
                    fieldCoordinates,
                    inputUsageCoordinates,
                    fragments,
                    schema,
                    activeFragments
            );
            return;
        }

        GraphQLType type = schema.getType(inlineFragment.getTypeCondition().getName());
        if (type instanceof GraphQLUnmodifiedType unmodifiedType && isSelectableParentType(unmodifiedType)) {
            collectFields(
                    unmodifiedType,
                    inlineFragment.getSelectionSet(),
                    fieldCoordinates,
                    inputUsageCoordinates,
                    fragments,
                    schema,
                    activeFragments
            );
        }
    }

    private static void collectFieldArguments(
            String fieldCoordinate,
            Field field,
            GraphQLFieldDefinition fieldDefinition,
            Set<InputUsageCoordinate> inputUsageCoordinates
    ) {
        for (Argument argument : field.getArguments()) {
            GraphQLArgument argumentDefinition = fieldDefinition.getArgument(argument.getName());
            if (argumentDefinition == null) {
                continue;
            }
            inputUsageCoordinates.add(new InputUsageCoordinate(
                    fieldCoordinate + "(" + argument.getName() + ":)",
                    InputUsageCoordinateKind.FIELD_ARGUMENT
            ));
            collectInputUsageFromAstValue(
                    argument.getValue(),
                    argumentDefinition.getType(),
                    inputUsageCoordinates
            );
        }
    }

    private static void collectInputUsageFromAstValue(
            Value<?> value,
            GraphQLInputType inputType,
            Set<InputUsageCoordinate> inputUsageCoordinates
    ) {
        GraphQLInputType unwrappedNonNull = inputTypeOf(GraphQLTypeUtil.unwrapNonNull(inputType));
        if (unwrappedNonNull instanceof GraphQLList listType) {
            if (value instanceof ArrayValue arrayValue) {
                for (Value<?> nestedValue : arrayValue.getValues()) {
                    collectInputUsageFromAstValue(
                            nestedValue,
                            inputTypeOf(listType.getWrappedType()),
                            inputUsageCoordinates
                    );
                }
            } else {
                collectInputUsageFromAstValue(
                        value,
                        inputTypeOf(listType.getWrappedType()),
                        inputUsageCoordinates
                );
            }
            return;
        }

        if (!(unwrappedNonNull instanceof GraphQLInputObjectType inputObjectType)
                || !(value instanceof ObjectValue objectValue)) {
            return;
        }

        for (var objectField : objectValue.getObjectFields()) {
            var inputFieldDefinition = inputObjectType.getFieldDefinition(objectField.getName());
            if (inputFieldDefinition == null) {
                continue;
            }
            inputUsageCoordinates.add(new InputUsageCoordinate(
                    inputObjectType.getName() + "." + objectField.getName(),
                    InputUsageCoordinateKind.INPUT_OBJECT_FIELD
            ));
            collectInputUsageFromAstValue(
                    objectField.getValue(),
                    inputFieldDefinition.getType(),
                    inputUsageCoordinates
            );
        }
    }

    private static GraphQLInputType inputTypeOf(GraphQLType type) {
        if (type instanceof GraphQLInputType inputType) {
            return inputType;
        }
        throw new IllegalArgumentException("Expected GraphQL input type but got " + type);
    }

    private static boolean isSelectableParentType(GraphQLUnmodifiedType type) {
        return type instanceof GraphQLObjectType
                || type instanceof GraphQLInterfaceType
                || type instanceof GraphQLUnionType;
    }

    record ProcessedUsage(
            @Nullable String operationName,
            String operationType,
            String canonicalDocument,
            List<String> fieldCoordinates,
            List<InputUsageCoordinate> inputUsageCoordinates
    ) {
    }

    record InputUsageCoordinate(
            String coordinate,
            InputUsageCoordinateKind kind
    ) {
    }

    private record UsageCoordinates(
            List<String> fieldCoordinates,
            List<InputUsageCoordinate> inputUsageCoordinates
    ) {
    }
}
