scio-koans
==========

[![Build Status](https://travis-ci.org/nevillelyh/scio-koans.svg?branch=master)](https://travis-ci.org/nevillelyh/scio-koans)
[![GitHub license](https://img.shields.io/github/license/nevillelyh/scio-koans.svg)](./LICENSE)

A collection of [Scio](https://github.com/spotify/scio) exercises inspired by [Ruby Koans](http://rubykoans.com/) and many others.

# Usage

Clone the repository and start the [sbt](https://www.scala-sbt.org/) console. You need to use Java 8 for now.

```bash
git clone https://github.com/nevillelyh/scio-koans.git
cd scio-koans
sbt
```

Run `~nextKoan` inside the console. This will watch your local files and run the next pending koan in repeat.

Fix any issues in the Koan to make tests pass. Replace any missing implementations like `???` or `?[T]` (something of type `T`) with your solution.

Once the test is green, and you are satisfied with the solution, remove the first line in the Koan, `ImNotDone`, to move on to the next one. You can also remove `ImNotDone` to skip a Koan.


Here are all the tasks available.

- `allKoans` - show all Koans and their status
- `nextKoan` - run the next pending Koan
- `test` - run all Koans
- `testOnly <koan>` - run a specific Koan, e.g. `testOnly scio.koans.a1_collections.K00_Jmh`

# License

Copyright 2020 Neville Li.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
