# GraphQL + Databacon

## Task Queue

- datahog_wrappers -> types
- consider the confluence of inheritance and unions/contexts
- settle on the new databacon interface


## Interface Design Questions

### Type System
- [ ] field definition interface
  - [ ] args & custom resolution logic
    - needs to expose a method for falling back on default datapork
    resolution logic. assumption is that if the custom resolution method
    is defined, it runs, but it may want to fall back...
  - [ ] description
  - [ ] non-null
  - [ ] deprecation (punt? seems like something that can be added post hoc)
- [ ] lists
- [x] interfaces
- [x] unions
  - UnionType = Tuple, Of, Other, Types
- [x] scalars (int/float/str/boolean/id) and aliases
- [x] enums
- migrations (e.g., what happens to old data, what happens to old operations, etc)

# Queries
- directives (i.e., interface for defining custom directives?)
- mutations

## Implementation Considerations
- schema mutation (implies a new schema DSL)
- relay paging compliance
- introspection (keep in mind to make it easy to respond to introspective queries)
  - example instrospective types https://facebook.github.io/graphql/#sec-Schema-Introspection

## Maybe
- flags
- node value
