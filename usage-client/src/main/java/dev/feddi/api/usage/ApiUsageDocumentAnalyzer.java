package dev.feddi.api.usage;

import dev.feddi.api.usage.v1.InputUsageCoordinateKind;
import graphql.execution.CoercedVariables;
import graphql.language.AstPrinter;
import graphql.language.AstSignature;
import graphql.language.AstSignatureWithInputResult;
import graphql.language.Document;
import graphql.language.OperationDefinition;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.stream.Stream;

final class ApiUsageDocumentAnalyzer {

    ProcessedUsage analyze(ApiUsageInvocation invocation) {
        var operation = findOperation(invocation.document(), invocation.operationName());
        var signatureResult = new AstSignature().signatureWithInput(
                invocation.document(),
                invocation.operationName(),
                invocation.schema(),
                CoercedVariables.of(invocation.variables())
        );
        var signatureDocument = signatureResult.getDocument();
        var canonicalDocument = AstPrinter.printAstCompact(signatureDocument);

        return new ProcessedUsage(
                operation.getName(),
                operation.getOperation().name(),
                canonicalDocument,
                signatureResult.getFieldCoordinates(),
                inputUsageCoordinates(signatureResult)
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

    private static List<InputUsageCoordinate> inputUsageCoordinates(AstSignatureWithInputResult signatureResult) {
        return Stream.of(
                        inputUsageCoordinates(signatureResult.getFieldArgumentCoordinates(), InputUsageCoordinateKind.FIELD_ARGUMENT),
                        inputUsageCoordinates(signatureResult.getUsedDirectives(), InputUsageCoordinateKind.USED_DIRECTIVE),
                        inputUsageCoordinates(signatureResult.getDirectiveArgumentCoordinates(), InputUsageCoordinateKind.DIRECTIVE_ARGUMENT),
                        inputUsageCoordinates(signatureResult.getInputObjectFieldCoordinates(), InputUsageCoordinateKind.INPUT_OBJECT_FIELD)
                )
                .flatMap(List::stream)
                .toList();
    }

    private static List<InputUsageCoordinate> inputUsageCoordinates(
            List<String> coordinates,
            InputUsageCoordinateKind kind
    ) {
        return coordinates.stream()
                .map(coordinate -> new InputUsageCoordinate(coordinate, kind))
                .toList();
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

}
