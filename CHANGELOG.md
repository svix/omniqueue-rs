# 0.2.0

This release is a big one, and we are considering omniqueue out of early development now.
You can expect the API to change much less in the coming releases compared to this one.

## Breaking changes

- **redis: Some implementation changes mean that this backend is runtime-incompatible with the same backend in omniqueue 0.1**
- Revise the public module structure to shorten import paths and make the docs easier to navigate
- Revise the public API to require fewer trait imports for common usage
- Rename a few types and traits
  - Most notably, `MemoryQueueBackend` is now named `InMemoryBackend`
  - Everything else should be easily found by searching for the old names
- Remove custom encoders / decoders
  - Custom encoding can be handled more efficiently by wrapping omniqueue's
    `raw` send / receive interfaces into a custom higher-level interface
- Update and prune dependency tree
- Switch omniqueue's public traits from `async_trait` to [native async-in-traits][]
- Simplify generic bounds (only matters if you were using omniqueue in generic code)

## Additions

- Add a backend for Google Cloud's Pub/Sub queue (`gcp_pubsub` feature / module)
- Add some documentation
- Introduce an `omniqueue::Result` type alias

[native async-in-traits]: https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits.html

# 0.1.0

Initial release.
