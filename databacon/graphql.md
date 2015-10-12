# GraphQL + Databacon

## Task/Thought Queue
Q: break the spec and allow unstructed data?
A: definitely in databacon, possibly in gql
   - might not be a break, at all, just a lack of a selection set


### Type System Design
- [x] field definition interface
  - [x] undefined type references
  - [x] args & custom resolution logic
    - raise DoTheDefaultThing
  - [x] description
  - [x] non-null
- [x] lists and relationship cardinality
- [x] interfaces
- [x] unions
- [x] scalars (int/float/str/boolean/id) and aliases
- [x] enums
- [ ] schema modification: migration, deprecation...
  - class X(Node):
      y = 1
      dep_warning(y, "Will be deprecated in favor of...")


# Queries
- directives (i.e., interface for defining custom directives?)
- mutations

## Spec Adherence
- gql enum can't have true/false/null


## Implementation Todo/Notes/Considerations
- possibly punt on non-prop field storage to simplify?
- schema mutation (implies a new schema DSL)
- relay paging compliance
- introspection (keep in mind to make it easy to respond to introspective queries)
  - example instrospective types https://facebook.github.io/graphql/#sec-Schema-Introspection
