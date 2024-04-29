<!-- insertion marker -->

<a name="v0.1.9"></a>

## [v0.1.9](https://github.com/thaeber/rdmlibpy/compare/v0.1.8...v0.1.9) (2024-04-29)

### Bug Fixes

- DataFrameWriteCSV -> Prevents blank lines from being written between rows of the data frame :bug: ([5825908](https://github.com/thaeber/rdmlibpy/commit/5825908276323d57a4d2b802d8b6cf86c53545f4))
- DataFrameWriteCSV => Fix writing attrs property when data frame contains pint columns :bug: ([f123de1](https://github.com/thaeber/rdmlibpy/commit/f123de1d0a741d87d29f1bc0e35f8f65b5e69507))
- Explicitly set utf-8 encoding when reading and writing text files :bug: ([7575ba0](https://github.com/thaeber/rdmlibpy/commit/7575ba04f1cfc9b38b92093f12f47ad2e714a9be))
- :bug: Explicitly set utf-8 encoding when reading and writing text files ([a2a9eff](https://github.com/thaeber/rdmlibpy/commit/a2a9effef583368f52b24fd55556a96ec50f011e))

<a name="v0.1.8"></a>

## [v0.1.8](https://github.com/thaeber/rdmlibpy/compare/v0.1.7...v0.1.8) (2024-04-28)

### Bug Fixes

- DataFrameFileCache => Preserve attrs when caching data frames ([483d9bb](https://github.com/thaeber/rdmlibpy/commit/483d9bbd39b8a191a446e8cbc9c802df9b0f4bf7))
- Create path to cache file if it does not exists ([80f7d17](https://github.com/thaeber/rdmlibpy/commit/80f7d17d2cb729637baa0efe81bf2e6bc6bbe897))
- Accidental return of dataframe without units when using dequantify=True ([45039d5](https://github.com/thaeber/rdmlibpy/commit/45039d50d16539bafdde22e920e8417b7eb8621b))
- Renamed "target" parameter to "filename" ([33d31e3](https://github.com/thaeber/rdmlibpy/commit/33d31e3011b78ab4999d8e7d21d781e2cc0840c3))
- Test for process id ([41203c0](https://github.com/thaeber/rdmlibpy/commit/41203c0169b2a6ce3ddf4a2c092b0566b29ed58c))
- Fixed typo in "dataframe.cache" process id (#9) ([f6acb32](https://github.com/thaeber/rdmlibpy/commit/f6acb32ef7c1026d6a1f4c0f956605ec1fc72d64))
- Renamed process to "dataframe.\[read|write\].csv" for consistency (#8) ([a9a7132](https://github.com/thaeber/rdmlibpy/commit/a9a7132bd19118838a1f4acbaf21b217692fd33b))
- DataFrameWriteCSV.run() did not return input value (#7) ([a297aa0](https://github.com/thaeber/rdmlibpy/commit/a297aa03a556fca1684b20fc2cda6c26855834cd))

### Features

- DataFrameWriteCSV => Write contents of attrs in the file header as comments ([c3f8da1](https://github.com/thaeber/rdmlibpy/commit/c3f8da14c14f2b4b1593deab076eb0e1fe7caf03))
- Storing metadata in the attrs dictionary of DataFrame ([8702ca8](https://github.com/thaeber/rdmlibpy/commit/8702ca8bbea2b76113b92b84479866927d9a1ba4))
- Automatic handling of data frames with units by default ([e8a02bb](https://github.com/thaeber/rdmlibpy/commit/e8a02bbd17caabd64ab0c1952d7f5d4a85c86d04))
- Added more granular handling of indices when writing to csv files. By default it now resets named indices before writing. ([0db9c17](https://github.com/thaeber/rdmlibpy/commit/0db9c17c5c2ba62f0f715709e2f461c9aa7d8d6a))

### Code Refactoring

- Renamed conda development environment to match package name ([59e098a](https://github.com/thaeber/rdmlibpy/commit/59e098afe94d64aae9ca46e4f08b4ffaf9cda896))

### Chore

- Adding and running pre-commit ([45f1b24](https://github.com/thaeber/rdmlibpy/commit/45f1b246393ad3ddf77b17327bf41d0d96231d8b))

<a name="v0.1.7"></a>

## [v0.1.7](https://github.com/thaeber/rdmlibpy/compare/v0.1.6...v0.1.7) (2024-04-23)

### Features

- Allow sequence of sequences when defining processes ([cf52463](https://github.com/thaeber/rdmlibpy/commit/cf524633b3708614ceda138c81d387c40f730b64))

### Chore

- Update changelog ([dcae5f0](https://github.com/thaeber/rdmlibpy/commit/dcae5f03fdabe0d8d153f73163bb610e9d4eb8ad))

<a name="v0.1.6"></a>

## [v0.1.6](https://github.com/thaeber/rdmlibpy/compare/v0.1.5...v0.1.6) (2024-04-23)

### Chore

- Update changelog ([2a066d7](https://github.com/thaeber/rdmlibpy/commit/2a066d71c1aad46d046bccff18123f00a16c86a9))
- Updated changelog ([b5ee656](https://github.com/thaeber/rdmlibpy/commit/b5ee6565c08dad644a411d8430320b0e951b40e9))

### Style

- Renamed project to "rdmlibpy" (#5) ([df40fa7](https://github.com/thaeber/rdmlibpy/commit/df40fa7494207692f01020649cc91599325adba8))

### Build

- Fixed invalid filename in bump-my-version config ([fc5a965](https://github.com/thaeber/rdmlibpy/commit/fc5a965e7c8a2960879a8807e8b4ba8cc37d5c30))

<a name="v0.1.5"></a>

## [v0.1.5](https://github.com/thaeber/rdmlibpy/compare/v0.1.3...v0.1.5) (2024-04-23)

### Style

- Renamed "select.\[columns|timespan\]" to "dataframe.select.\[columns|timespan\]" (#4) ([eb32028](https://github.com/thaeber/rdmlibpy/commit/eb320282b9696a6c1fbe45e44f76f00a84604603))

<a name="v0.1.3"></a>

## [v0.1.3](https://github.com/thaeber/rdmlibpy/compare/v0.1.2...v0.1.3) (2024-04-19)

<a name="v0.1.2"></a>

## [v0.1.2](https://github.com/thaeber/rdmlibpy/compare/v0.1.1...v0.1.2) (2024-04-02)

<a name="v0.1.1"></a>

## [v0.1.1](https://github.com/thaeber/rdmlibpy/compare/b6e05adfa1b72a75295601854b5caaedc1876993...v0.1.1) (2024-04-01)
