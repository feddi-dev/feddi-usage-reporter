package dev.feddi.api.usage;

import dev.feddi.api.usage.v1.InputUsageCoordinateKind;
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
import graphql.language.VariableDefinition;
import graphql.language.VariableReference;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLModifiedType;
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

public final class ApiUsageDocumentAnalyzer {

    public ProcessedUsage analyze(ApiUsageInvocation invocation) {
        var operation = findOperation(invocation.document(), invocation.operationName());
        var normalized = new AstSignature().signatureQuery(invocation.document(), invocation.operationName());
        var canonicalDocument = AstPrinter.printAstCompact(normalized);
        var usageCoordinates = extractUsageCoordinates(
                operation,
                invocation.document(),
                invocation.schema(),
                invocation.variables()
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
            GraphQLSchema schema,
            Map<String, @Nullable Object> variables
    ) {
        Set<String> fieldCoordinates = new LinkedHashSet<>();
        Set<InputUsageCoordinate> inputUsageCoordinates = new LinkedHashSet<>();
        var fragments = document.getDefinitionsOfType(FragmentDefinition.class).stream()
                .collect(Collectors.toMap(FragmentDefinition::getName, fragment -> fragment));
        var variableDefinitions = operation.getVariableDefinitions().stream()
                .collect(Collectors.toMap(VariableDefinition::getName, variableDefinition -> variableDefinition));

        String rootTypeName = switch (operation.getOperation()) {
            case QUERY -> "Query";
            case MUTATION -> "Mutation";
            case SUBSCRIPTION -> "Subscription";
        };

        GraphQLObjectType rootType = schema.getObjectType(rootTypeName);
        if (rootType == null) {
            return new UsageCoordinates(List.of(), List.of());
        }

        collectFields(
                rootType,
                operation.getSelectionSet(),
                fieldCoordinates,
                inputUsageCoordinates,
                fragments,
                variableDefinitions,
                variables,
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
            Map<String, VariableDefinition> variableDefinitions,
            Map<String, @Nullable Object> variables,
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
                        variableDefinitions,
                        variables,
                        schema,
                        activeFragments
                );
            } else if (selection instanceof FragmentSpread spread) {
                collectFragment(
                        spread,
                        fieldCoordinates,
                        inputUsageCoordinates,
                        fragments,
                        variableDefinitions,
                        variables,
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
                        variableDefinitions,
                        variables,
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
            Map<String, VariableDefinition> variableDefinitions,
            Map<String, @Nullable Object> variables,
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
                    inputUsageCoordinates,
                    variableDefinitions,
                    variables
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
                    variableDefinitions,
                    variables,
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
            Map<String, VariableDefinition> variableDefinitions,
            Map<String, @Nullable Object> variables,
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
                        variableDefinitions,
                        variables,
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
            Map<String, VariableDefinition> variableDefinitions,
            Map<String, @Nullable Object> variables,
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
                    variableDefinitions,
                    variables,
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
                    variableDefinitions,
                    variables,
                    schema,
                    activeFragments
            );
        }
    }

    private static void collectFieldArguments(
            String fieldCoordinate,
            Field field,
            GraphQLFieldDefinition fieldDefinition,
            Set<InputUsageCoordinate> inputUsageCoordinates,
            Map<String, VariableDefinition> variableDefinitions,
            Map<String, @Nullable Object> variables
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
                    inputUsageCoordinates,
                    variableDefinitions,
                    variables
            );
        }
    }

    private static void collectInputUsageFromAstValue(
            Value<?> value,
            GraphQLInputType inputType,
            Set<InputUsageCoordinate> inputUsageCoordinates,
            Map<String, VariableDefinition> variableDefinitions,
            Map<String, @Nullable Object> variables
    ) {
        if (value instanceof VariableReference variableReference) {
            String variableName = variableReference.getName();
            if (variables.containsKey(variableName)) {
                collectInputUsageFromJavaValue(variables.get(variableName), inputType, inputUsageCoordinates);
                return;
            }
            VariableDefinition variableDefinition = variableDefinitions.get(variableName);
            if (variableDefinition != null && variableDefinition.getDefaultValue() != null) {
                collectInputUsageFromAstValue(
                        variableDefinition.getDefaultValue(),
                        inputType,
                        inputUsageCoordinates,
                        variableDefinitions,
                        variables
                );
            }
            return;
        }

        GraphQLInputType unwrappedNonNull = unwrapNonNull(inputType);
        if (unwrappedNonNull instanceof GraphQLList listType) {
            if (value instanceof ArrayValue arrayValue) {
                for (Value<?> nestedValue : arrayValue.getValues()) {
                    collectInputUsageFromAstValue(
                            nestedValue,
                            inputTypeOf(listType.getWrappedType()),
                            inputUsageCoordinates,
                            variableDefinitions,
                            variables
                    );
                }
            } else {
                collectInputUsageFromAstValue(
                        value,
                        inputTypeOf(listType.getWrappedType()),
                        inputUsageCoordinates,
                        variableDefinitions,
                        variables
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
                    inputUsageCoordinates,
                    variableDefinitions,
                    variables
            );
        }
    }

    private static void collectInputUsageFromJavaValue(
            @Nullable Object value,
            GraphQLInputType inputType,
            Set<InputUsageCoordinate> inputUsageCoordinates
    ) {
        if (value == null) {
            return;
        }

        GraphQLInputType unwrappedNonNull = unwrapNonNull(inputType);
        if (unwrappedNonNull instanceof GraphQLList listType) {
            if (value instanceof Iterable<?> iterable) {
                for (Object nestedValue : iterable) {
                    collectInputUsageFromJavaValue(
                            nestedValue,
                            inputTypeOf(listType.getWrappedType()),
                            inputUsageCoordinates
                    );
                }
            } else if (value.getClass().isArray()) {
                int length = java.lang.reflect.Array.getLength(value);
                for (int i = 0; i < length; i++) {
                    collectInputUsageFromJavaValue(
                            java.lang.reflect.Array.get(value, i),
                            inputTypeOf(listType.getWrappedType()),
                            inputUsageCoordinates
                    );
                }
            } else {
                collectInputUsageFromJavaValue(
                        value,
                        inputTypeOf(listType.getWrappedType()),
                        inputUsageCoordinates
                );
            }
            return;
        }

        if (!(unwrappedNonNull instanceof GraphQLInputObjectType inputObjectType)
                || !(value instanceof Map<?, ?> objectValue)) {
            return;
        }

        for (var entry : objectValue.entrySet()) {
            if (!(entry.getKey() instanceof String fieldName)) {
                continue;
            }
            var inputFieldDefinition = inputObjectType.getFieldDefinition(fieldName);
            if (inputFieldDefinition == null) {
                continue;
            }
            inputUsageCoordinates.add(new InputUsageCoordinate(
                    inputObjectType.getName() + "." + fieldName,
                    InputUsageCoordinateKind.INPUT_OBJECT_FIELD
            ));
            collectInputUsageFromJavaValue(
                    entry.getValue(),
                    inputFieldDefinition.getType(),
                    inputUsageCoordinates
            );
        }
    }

    private static GraphQLInputType unwrapNonNull(GraphQLInputType inputType) {
        GraphQLType type = inputType;
        while (type instanceof GraphQLModifiedType modifiedType && GraphQLTypeUtil.isNonNull(type)) {
            type = modifiedType.getWrappedType();
        }
        return inputTypeOf(type);
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

    public record ProcessedUsage(
            @Nullable String operationName,
            String operationType,
            String canonicalDocument,
            List<String> fieldCoordinates,
            List<InputUsageCoordinate> inputUsageCoordinates
    ) {
    }

    public record InputUsageCoordinate(
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
