# Changelog

## v2021.6.7

([full changelog](https://github.com/NCAR/ecgtools/compare/e5108a4edfef211b64dcb69a28f57247e717237b...fbc41f23244798fe90782cbea672b21fee0d373b))

## Merged PRs

- Add `postprocess` func [#40](https://github.com/NCAR/ecgtools/pull/40) ([@andersy005](https://github.com/andersy005))
- Add version to CMIP6 parser [#39](https://github.com/NCAR/ecgtools/pull/39) ([@andersy005](https://github.com/andersy005))
- Add pop 5 day stream [#38](https://github.com/NCAR/ecgtools/pull/38) ([@mgrover1](https://github.com/mgrover1))
- Add timeseries parser [#37](https://github.com/NCAR/ecgtools/pull/37) ([@mgrover1](https://github.com/mgrover1))
- add time_range attribute to cmip6 parser [#36](https://github.com/NCAR/ecgtools/pull/36) ([@mgrover1](https://github.com/mgrover1))
- Use relative path when saving `catalog_file` entry [#35](https://github.com/NCAR/ecgtools/pull/35) ([@andersy005](https://github.com/andersy005))
- Update CESM History File Example, Fix stream issue [#33](https://github.com/NCAR/ecgtools/pull/33) ([@mgrover1](https://github.com/mgrover1))
- Fix variable column name [#32](https://github.com/NCAR/ecgtools/pull/32) ([@mgrover1](https://github.com/mgrover1))
- Add aggregation_control key [#31](https://github.com/NCAR/ecgtools/pull/31) ([@andersy005](https://github.com/andersy005))
- Fix casename issue [#30](https://github.com/NCAR/ecgtools/pull/30) ([@mgrover1](https://github.com/mgrover1))
- Extract frequency from file [#27](https://github.com/NCAR/ecgtools/pull/27) ([@mgrover1](https://github.com/mgrover1))
- Add docs placeholders [#26](https://github.com/NCAR/ecgtools/pull/26) ([@andersy005](https://github.com/andersy005))
- Add CMIP6 example [#25](https://github.com/NCAR/ecgtools/pull/25) ([@andersy005](https://github.com/andersy005))
- Remove old code [#24](https://github.com/NCAR/ecgtools/pull/24) ([@andersy005](https://github.com/andersy005))
- Add save method to main Builder class [#23](https://github.com/NCAR/ecgtools/pull/23) ([@andersy005](https://github.com/andersy005))
- Deal with CESM history file times in more generic way [#22](https://github.com/NCAR/ecgtools/pull/22) ([@mgrover1](https://github.com/mgrover1))
- Add `__post_init_post_parse__` method to `Builder` [#20](https://github.com/NCAR/ecgtools/pull/20) ([@andersy005](https://github.com/andersy005))
- Rewrite `Builder` without dask dependencies [#19](https://github.com/NCAR/ecgtools/pull/19) ([@andersy005](https://github.com/andersy005))
- Add parser for CESM history files [#18](https://github.com/NCAR/ecgtools/pull/18) ([@andersy005](https://github.com/andersy005))
- Disable YAMALE YAML based approach [#17](https://github.com/NCAR/ecgtools/pull/17) ([@andersy005](https://github.com/andersy005))
- Remove CMIP6 default parser [#14](https://github.com/NCAR/ecgtools/pull/14) ([@andersy005](https://github.com/andersy005))
- Bump pre-commit/action from v2.0.2 to v2.0.3 [#13](https://github.com/NCAR/ecgtools/pull/13) ([@dependabot](https://github.com/dependabot))
- Add build-from-config command [#12](https://github.com/NCAR/ecgtools/pull/12) ([@andersy005](https://github.com/andersy005))
- Add CMIP6 parser/builder [#11](https://github.com/NCAR/ecgtools/pull/11) ([@andersy005](https://github.com/andersy005))
- 🔨 Code refactoring [#10](https://github.com/NCAR/ecgtools/pull/10) ([@andersy005](https://github.com/andersy005))
- Turn `Builder` class into a data class [#9](https://github.com/NCAR/ecgtools/pull/9) ([@andersy005](https://github.com/andersy005))
- Add builder for CESM2 Seasonal-to-Multiyear Large Ensemble (SMYLE) [#8](https://github.com/NCAR/ecgtools/pull/8) ([@andersy005](https://github.com/andersy005))
- 👷 CI overhaul [#7](https://github.com/NCAR/ecgtools/pull/7) ([@andersy005](https://github.com/andersy005))
- Update isort gh action and pre-commit hook [#6](https://github.com/NCAR/ecgtools/pull/6) ([@andersy005](https://github.com/andersy005))
- Generic interface [#5](https://github.com/NCAR/ecgtools/pull/5) ([@sherimickelson](https://github.com/sherimickelson))
- Add core functionality for parsing attributes from an open file [#3](https://github.com/NCAR/ecgtools/pull/3) ([@andersy005](https://github.com/andersy005))

### Contributors to this release

([GitHub contributors page for this release](https://github.com/NCAR/ecgtools/graphs/contributors?from=2020-06-01&to=2021-06-08&type=c))

[@andersy005](https://github.com/search?q=repo%3ANCAR%2Fecgtools+involves%3Aandersy005+updated%3A2020-06-01..2021-06-08&type=Issues) | [@billsacks](https://github.com/search?q=repo%3ANCAR%2Fecgtools+involves%3Abillsacks+updated%3A2020-06-01..2021-06-08&type=Issues) | [@dependabot](https://github.com/search?q=repo%3ANCAR%2Fecgtools+involves%3Adependabot+updated%3A2020-06-01..2021-06-08&type=Issues) | [@kmpaul](https://github.com/search?q=repo%3ANCAR%2Fecgtools+involves%3Akmpaul+updated%3A2020-06-01..2021-06-08&type=Issues) | [@mgrover1](https://github.com/search?q=repo%3ANCAR%2Fecgtools+involves%3Amgrover1+updated%3A2020-06-01..2021-06-08&type=Issues) | [@sherimickelson](https://github.com/search?q=repo%3ANCAR%2Fecgtools+involves%3Asherimickelson+updated%3A2020-06-01..2021-06-08&type=Issues)
