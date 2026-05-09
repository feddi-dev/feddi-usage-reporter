package dev.feddi.api.usage;

import dev.feddi.api.usage.v1.InputUsageCoordinateKind;
import graphql.parser.Parser;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiUsageDocumentAnalyzerTest {

    private static final GraphQLSchema SCHEMA = schema();

    private final ApiUsageDocumentAnalyzer analyzer = new ApiUsageDocumentAnalyzer();

    @Test
    void analyze_extractsScalarEnumObjectFieldsAndNamedFragments() {
        var usage = analyze("ViewerDashboard", """
                query ViewerDashboard($includeFriend: Boolean!) {
                  viewer {
                    id
                    name
                    status
                    profile {
                      bio
                      avatar {
                        url
                      }
                    }
                    ...FriendFields @include(if: $includeFriend)
                  }
                }

                fragment FriendFields on User {
                  friend {
                    id
                    name
                    status
                  }
                }
                """);

        assertThat(usage.operationName()).isEqualTo("ViewerDashboard");
        assertThat(usage.operationType()).isEqualTo("QUERY");
        assertThat(usage.canonicalDocument()).isNotBlank();
        assertThat(usage.fieldCoordinates()).containsExactly(
                "Query.viewer",
                "User.id",
                "User.name",
                "User.profile",
                "Profile.avatar",
                "Image.url",
                "Profile.bio",
                "User.status",
                "User.friend"
        );
    }

    @Test
    void analyze_extractsInlineFragmentsWithAndWithoutTypeConditions() {
        var usage = analyze("InlineFragments", """
                query InlineFragments {
                  viewer {
                    __typename
                    ... {
                      id
                      profile {
                        bio
                      }
                    }
                    ... on User {
                      friend {
                        name
                      }
                    }
                  }
                }
                """);

        assertThat(usage.fieldCoordinates()).containsExactly(
                "Query.viewer",
                "User.id",
                "User.profile",
                "Profile.bio",
                "User.friend",
                "User.name"
        );
    }

    @Test
    void analyze_extractsInterfaceAndUnionSelections() {
        var usage = analyze("InterfaceAndUnion", """
                query InterfaceAndUnion {
                  node(id: "1") {
                    __typename
                    ...NodeFields
                    ... on User {
                      name
                      status
                    }
                    ... on Product {
                      sku
                      price
                      owner {
                        id
                      }
                    }
                  }
                  search(text: "a") {
                    ...SearchResultFields
                  }
                }

                fragment NodeFields on Node {
                  id
                }

                fragment SearchResultFields on SearchResult {
                  ... on User {
                    friend {
                      name
                    }
                  }
                  ... on Product {
                    displayName
                  }
                }
                """);

        assertThat(usage.fieldCoordinates()).containsExactly(
                "Query.node",
                "Node.id",
                "Product.owner",
                "User.id",
                "Product.price",
                "Product.sku",
                "User.name",
                "User.status",
                "Query.search",
                "Product.displayName",
                "User.friend"
        );
    }

    @Test
    void analyze_breaksRecursiveFragmentCycles() {
        var usage = analyze("RecursiveFragments", """
                query RecursiveFragments {
                  viewer {
                    ...UserA
                  }
                }

                fragment UserA on User {
                  id
                  friend {
                    ...UserB
                  }
                }

                fragment UserB on User {
                  name
                  friend {
                    ...UserA
                  }
                }
                """);

        assertThat(usage.fieldCoordinates()).containsExactly(
                "Query.viewer",
                "User.friend",
                "User.name",
                "User.id"
        );
    }

    @Test
    void analyze_reportsRepeatedFieldSelectionsExactlyOnce() {
        var usage = analyze("DuplicateFields", """
                query DuplicateFields {
                  viewer {
                    id
                    id
                    userId: id
                    name
                    ... {
                      name
                      profile {
                        bio
                        bio
                      }
                    }
                    ... on User {
                      id
                      profile {
                        bio
                      }
                    }
                    ...RepeatedUserFields
                    ...RepeatedUserFields
                  }
                }

                fragment RepeatedUserFields on User {
                  id
                  name
                  profile {
                    bio
                  }
                }
                """);

        assertThat(usage.fieldCoordinates()).containsExactly(
                "Query.viewer",
                "User.id",
                "User.name",
                "User.profile",
                "Profile.bio"
        );
        assertThat(usage.fieldCoordinates()).doesNotHaveDuplicates();
    }

    @Test
    void analyze_reportsRepeatedFieldsAcrossInterfaceAndUnionSelectionsExactlyOnce() {
        var usage = analyze("DuplicateAbstractFields", """
                query DuplicateAbstractFields {
                  node(id: "1") {
                    ...NodeFields
                    ...NodeFields
                    ... on Node {
                      id
                    }
                    ... on User {
                      id
                      name
                      name
                    }
                  }
                  search(text: "a") {
                    ...SearchResultFields
                    ...SearchResultFields
                    ... on Product {
                      displayName
                      displayName
                    }
                  }
                }

                fragment NodeFields on Node {
                  id
                }

                fragment SearchResultFields on SearchResult {
                  ... on User {
                    id
                    name
                  }
                  ... on Product {
                    displayName
                  }
                }
                """);

        assertThat(usage.fieldCoordinates()).containsExactly(
                "Query.node",
                "Node.id",
                "User.id",
                "User.name",
                "Query.search",
                "Product.displayName"
        );
        assertThat(usage.fieldCoordinates()).doesNotHaveDuplicates();
    }

    @Test
    void analyze_usesSelectedOperationWhenDocumentContainsMultipleOperations() {
        var usage = analyze("ChangeStatus", """
                query Viewer {
                  viewer {
                    id
                  }
                }

                mutation ChangeStatus {
                  updateUserStatus(id: "1", status: ACTIVE) {
                    status
                    profile {
                      bio
                    }
                  }
                }
                """);

        assertThat(usage.operationName()).isEqualTo("ChangeStatus");
        assertThat(usage.operationType()).isEqualTo("MUTATION");
        assertThat(usage.fieldCoordinates()).containsExactly(
                "Mutation.updateUserStatus",
                "User.profile",
                "Profile.bio",
                "User.status"
        );
    }

    @Test
    void analyze_extractsFieldArgumentsAndInlineInputObjectFields() {
        var usage = analyze("InputUsage", """
                query InputUsage {
                  viewer(
                    filter: {
                      status: ACTIVE
                      profile: {
                        bioContains: "hello"
                        avatar: { size: 64 }
                      }
                    }
                    tags: [
                      { value: "internal", metadata: { source: "test" } }
                    ]
                  ) {
                    id
                  }
                }
                """);

        assertThat(coordinatesOfKind(usage, InputUsageCoordinateKind.FIELD_ARGUMENT)).containsExactly(
                "Query.viewer(filter:)",
                "Query.viewer(tags:)"
        );
        assertThat(coordinatesOfKind(usage, InputUsageCoordinateKind.INPUT_OBJECT_FIELD)).containsExactly(
                "UserFilter.profile",
                "ProfileFilter.avatar",
                "AvatarFilter.size",
                "ProfileFilter.bioContains",
                "UserFilter.status",
                "TagInput.metadata",
                "TagMetadataInput.source",
                "TagInput.value"
        );
    }

    @Test
    void analyze_extractsInputObjectFieldsFromRuntimeVariables() {
        var variables = new LinkedHashMap<String, Object>();
        variables.put("term", "product");
        var productFilter = new LinkedHashMap<String, Object>();
        productFilter.put("sku", "SKU-1");
        productFilter.put("minPrice", 100);
        var searchFilter = new LinkedHashMap<String, Object>();
        searchFilter.put("product", productFilter);
        variables.put("filter", searchFilter);

        var usage = analyze("SearchWithVariables", """
                query SearchWithVariables($term: String!, $filter: SearchFilter) {
                  search(text: $term, filter: $filter) {
                    ... on Product {
                      sku
                    }
                  }
                }
                """, variables);

        assertThat(coordinatesOfKind(usage, InputUsageCoordinateKind.FIELD_ARGUMENT)).containsExactly(
                "Query.search(filter:)",
                "Query.search(text:)"
        );
        assertThat(coordinatesOfKind(usage, InputUsageCoordinateKind.INPUT_OBJECT_FIELD)).containsExactly(
                "SearchFilter.product",
                "ProductFilter.minPrice",
                "ProductFilter.sku"
        );
    }

    @Test
    void analyze_operationSignatureIncludesSuppliedInputShape() {
        var usageWithStatusOnly = analyze("InputShape", """
                query InputShape {
                  viewer(filter: { status: ACTIVE }) {
                    id
                  }
                }
                """);
        var usageWithNestedProfile = analyze("InputShape", """
                query InputShape {
                  viewer(filter: { status: ACTIVE, profile: { bioContains: "hello" } }) {
                    id
                  }
                }
                """);

        assertThat(usageWithStatusOnly.canonicalDocument())
                .isNotEqualTo(usageWithNestedProfile.canonicalDocument());
        assertThat(coordinatesOfKind(usageWithStatusOnly, InputUsageCoordinateKind.INPUT_OBJECT_FIELD))
                .containsExactly("UserFilter.status");
        assertThat(coordinatesOfKind(usageWithNestedProfile, InputUsageCoordinateKind.INPUT_OBJECT_FIELD))
                .containsExactly("UserFilter.profile", "ProfileFilter.bioContains", "UserFilter.status");
    }

    @Test
    void analyze_omitsAbsentVariableInputFieldsFromSignatureAndCoordinates() {
        var usage = analyze("OptionalInput", """
                query OptionalInput($filter: UserFilter) {
                  viewer(filter: $filter) {
                    id
                  }
                }
                """);

        assertThat(coordinatesOfKind(usage, InputUsageCoordinateKind.FIELD_ARGUMENT)).isEmpty();
        assertThat(coordinatesOfKind(usage, InputUsageCoordinateKind.INPUT_OBJECT_FIELD)).isEmpty();
    }

    @Test
    void analyze_reportsRepeatedInputUsageCoordinatesExactlyOnce() {
        var usage = analyze("RepeatedInputUsage", """
                query RepeatedInputUsage {
                  first: viewer(filter: { status: ACTIVE }) {
                    id
                  }
                  second: viewer(filter: { status: ACTIVE }) {
                    id
                  }
                }
                """);

        assertThat(coordinatesOfKind(usage, InputUsageCoordinateKind.FIELD_ARGUMENT)).containsExactly(
                "Query.viewer(filter:)"
        );
        assertThat(coordinatesOfKind(usage, InputUsageCoordinateKind.INPUT_OBJECT_FIELD)).containsExactly(
                "UserFilter.status"
        );
        assertThat(usage.inputUsageCoordinates()).doesNotHaveDuplicates();
    }

    @Test
    void analyze_requiresOperationNameWhenDocumentContainsMultipleOperations() {
        assertThatThrownBy(() -> analyze(null, """
                query Viewer {
                  viewer {
                    id
                  }
                }

                query Search {
                  search(text: "a") {
                    ... on User {
                      id
                    }
                  }
                }
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("operationName is required when the document contains multiple operations");
    }

    private ApiUsageDocumentAnalyzer.ProcessedUsage analyze(@Nullable String operationName, String documentBody) {
        return analyze(operationName, documentBody, Map.of());
    }

    private ApiUsageDocumentAnalyzer.ProcessedUsage analyze(
            @Nullable String operationName,
            String documentBody,
            Map<String, Object> variables
    ) {
        return analyzer.analyze(ApiUsageInvocation.builder()
                .document(Parser.parse(documentBody))
                .operationName(operationName)
                .schema(SCHEMA)
                .variables(variables)
                .build());
    }

    private static java.util.List<String> coordinatesOfKind(
            ApiUsageDocumentAnalyzer.ProcessedUsage usage,
            InputUsageCoordinateKind kind
    ) {
        return usage.inputUsageCoordinates().stream()
                .filter(coordinate -> coordinate.kind() == kind)
                .map(ApiUsageDocumentAnalyzer.InputUsageCoordinate::coordinate)
                .toList();
    }

    private static GraphQLSchema schema() {
        var registry = new SchemaParser().parse("""
                interface Node {
                  id: ID!
                }

                interface Named {
                  displayName: String!
                }

                enum UserStatus {
                  ACTIVE
                  DISABLED
                }

                union SearchResult = User | Product

	                type Query {
	                  viewer(filter: UserFilter, tags: [TagInput!]): User!
	                  node(id: ID!): Node
	                  search(text: String, filter: SearchFilter): [SearchResult!]!
	                }

	                type Mutation {
	                  updateUserStatus(id: ID!, status: UserStatus!, input: UpdateUserInput): User
	                }

	                input UserFilter {
	                  status: UserStatus
	                  profile: ProfileFilter
	                }

	                input ProfileFilter {
	                  bioContains: String
	                  avatar: AvatarFilter
	                }

	                input AvatarFilter {
	                  size: Int
	                }

	                input TagInput {
	                  value: String!
	                  metadata: TagMetadataInput
	                }

	                input TagMetadataInput {
	                  source: String
	                }

	                input SearchFilter {
	                  product: ProductFilter
	                }

	                input ProductFilter {
	                  sku: String
	                  minPrice: Int
	                }

	                input UpdateUserInput {
	                  status: UserStatus!
	                  profile: ProfileFilter
	                }

                type Subscription {
                  userEvents: User
                }

                type User implements Node & Named {
                  id: ID!
                  displayName: String!
                  name: String!
                  status: UserStatus!
                  profile: Profile
                  friend: User
                  favoriteProduct: Product
                }

                type Product implements Node & Named {
                  id: ID!
                  displayName: String!
                  sku: String!
                  price: Int!
                  owner: User
                }

                type Profile {
                  bio: String!
                  avatar: Image
                }

                type Image {
                  url: String!
                }
                """);
        var wiring = RuntimeWiring.newRuntimeWiring()
                .type("Node", typeWiring -> typeWiring.typeResolver(environment -> null))
                .type("Named", typeWiring -> typeWiring.typeResolver(environment -> null))
                .type("SearchResult", typeWiring -> typeWiring.typeResolver(environment -> null))
                .build();
        return new SchemaGenerator().makeExecutableSchema(registry, wiring);
    }
}
