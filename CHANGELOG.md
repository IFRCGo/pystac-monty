# Changelog

## [0.2.6](https://github.com/IFRCGo/pystac-monty/compare/v0.2.5...v0.2.6) (2026-09-22)


### Bug Fixes

* **cems:** hazard mappings fixtures ([#249](https://github.com/IFRCGo/pystac-monty/issues/249)) ([6e15b5c](https://github.com/IFRCGo/pystac-monty/commit/6e15b5c955baf84469dbffb06b62169b049a5292))
* **cems:** key acquisition item id off image fileName, dedup within an AOI ([#251](https://github.com/IFRCGo/pystac-monty/issues/251)) ([e298b27](https://github.com/IFRCGo/pystac-monty/commit/e298b276d3d347be54fcd1368833222337fe3c94))
* **cems:** lowercase the acquisition item id ([#253](https://github.com/IFRCGo/pystac-monty/issues/253)) ([c8055d4](https://github.com/IFRCGo/pystac-monty/commit/c8055d4768ef6fa51b88b80489317a04f9265aea))

## [0.2.5](https://github.com/IFRCGo/pystac-monty/compare/v0.2.4...v0.2.5) (2026-09-18)


### Bug Fixes

* **cems:** dedupe hazard item_ids that collapse to the same slug. ([#238](https://github.com/IFRCGo/pystac-monty/issues/238)) ([448e45f](https://github.com/IFRCGo/pystac-monty/commit/448e45facf3c708a3e36500700676052485eda8b))
* **cems:** fix test case ([100c5c7](https://github.com/IFRCGo/pystac-monty/commit/100c5c7328a6402af336683588aada0e9160d9c9))
* **cems:** images metadata. ([#219](https://github.com/IFRCGo/pystac-monty/issues/219)) ([8fe834c](https://github.com/IFRCGo/pystac-monty/commit/8fe834c241a12957682aa931379cc073a25cfc97))
* **charter:** fix test case ([6149083](https://github.com/IFRCGo/pystac-monty/commit/61490833ae3469549be7c02b506a0172a79d734f))
* **charter:** handle calibrated datasets with no datetime value ([60f3235](https://github.com/IFRCGo/pystac-monty/commit/60f323500079a8be51ef4e75ac045705f690235f))
* **GIDD, IDU:** use partition files to handle transformations ([4f37749](https://github.com/IFRCGo/pystac-monty/commit/4f3774910aec4c9308587bc741f6dee9fdf23847))
* **tests:** avoid ifrc db connection for unit tests. ([#239](https://github.com/IFRCGo/pystac-monty/issues/239)) ([4d5f5bf](https://github.com/IFRCGo/pystac-monty/commit/4d5f5bfd3f06ca4d49b3b07418430ba74e64e064))
* update the hazard code mappings ([f723aad](https://github.com/IFRCGo/pystac-monty/commit/f723aadafd022a239cb88f7ed351362775caa996))
* update the ids for IDU and GIDD ([d66aa5a](https://github.com/IFRCGo/pystac-monty/commit/d66aa5a918b64d8be5f456703af26b7f1c44bae0))

## [0.2.4](https://github.com/IFRCGo/pystac-monty/compare/v0.2.3...v0.2.4) (2026-09-11)


### Bug Fixes

* **cems:** handle null geometry in response item creation ([37cd704](https://github.com/IFRCGo/pystac-monty/commit/37cd704f4214b51e9984464566d183ce8c91534d))
* **collections-href:** use the GitHub href for collection links in STAC items, regardless of how collections are fetched ([b2690ea](https://github.com/IFRCGo/pystac-monty/commit/b2690eaa8b676cf539b8bf9131c9a4f795b3f95e))
* **emdat:** update the hazard code mappings ([503648b](https://github.com/IFRCGo/pystac-monty/commit/503648b4ba3cb604e6c5187610f0fa15ca15641b))
* restore monty-stac-extension submodule pointer to current main ([11e7566](https://github.com/IFRCGo/pystac-monty/commit/11e756680344cd2b5752f19f73ade9ef497c8b3f))
* simplify assertion error message formatting ([fe35ab9](https://github.com/IFRCGo/pystac-monty/commit/fe35ab9814edcd37068a1955310a3750b4e78b89))

## [0.2.3](https://github.com/IFRCGo/pystac-monty/compare/v0.2.2...v0.2.3) (2026-09-10)


### Bug Fixes

* **cems:** fix cems stac href link ([ab1ba92](https://github.com/IFRCGo/pystac-monty/commit/ab1ba92935e99cd759f1b2d2e7389d270284d730))

## [0.2.2](https://github.com/IFRCGo/pystac-monty/compare/v0.2.1...v0.2.2) (2026-09-09)


### Bug Fixes

* **cems:** missing hazard rules. ([8b603af](https://github.com/IFRCGo/pystac-monty/commit/8b603af7062e150c5c35470e69b133d921fff739))

## [0.2.1](https://github.com/IFRCGo/pystac-monty/compare/v0.2.0...v0.2.1) (2026-09-08)


### Bug Fixes

* **CEMS:** add country name mapping for geocoder mismatches ([33cea6e](https://github.com/IFRCGo/pystac-monty/commit/33cea6eb49d7d013f7b7b24f38f884fbf4a001e9))

## [0.2.0](https://github.com/IFRCGo/pystac-monty/compare/v0.1.1...v0.2.0) (2026-09-08)


### Features

* add the transformer versions to all the sources ([af27d3e](https://github.com/IFRCGo/pystac-monty/commit/af27d3e2597bd21a6608f01752e2a91fb7fcc41f))
* **test-cases:** update the test cases for tranformer versions for all ([4c4501f](https://github.com/IFRCGo/pystac-monty/commit/4c4501fc93eb8bc22da98c36a824d77f62177e1b))

## [0.1.1](https://github.com/IFRCGo/pystac-monty/compare/v0.1.0...v0.1.1) (2026-09-01)


### Bug Fixes

* **cems:** fix test case due to change in stac-item links ([fb33f3a](https://github.com/IFRCGo/pystac-monty/commit/fb33f3a5f2fb0fac1c3a1a5b62a1fca5af926e68))
* **copernicus:** fix stac item related link ([4eee834](https://github.com/IFRCGo/pystac-monty/commit/4eee8342ab97735d9661bb2bc47d7a7bacfc6f0e))
* **desinventar:** update the hazard names mapping and hazard codes ma… ([#201](https://github.com/IFRCGo/pystac-monty/issues/201)) ([0163b3e](https://github.com/IFRCGo/pystac-monty/commit/0163b3e391fdc18e1f001ecb188e0d3b3bfa7cb0))
* **ibtracs:** general fixes and improvements to the transformer ([#203](https://github.com/IFRCGo/pystac-monty/issues/203)) ([765c9e2](https://github.com/IFRCGo/pystac-monty/commit/765c9e2197968fbbc0e795207f959148de144588))
