# Changelog

All notable changes to this project will be documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

Versioning for this project is based on [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.0.11-alpha - 2025-01-20

### Features

- Add a helper function to additionally shrink the chunk.

## v0.0.10-alpha - 2025-01-18

### Features

- Make the iterator reversible.

## v0.0.9-alpha - 2025-01-18

### Features

- Add `no_elements` function for convenience.

## v0.0.8-alpha - 2025-01-18

### Features

- Add `last` and `first` functions for convenience.

## v0.0.7-alpha - 2025-01-17

### Bugs

- Fix bug around setting next index internally after pruning

## v0.0.6-alpha - 2025-01-16

### Bugs

- Fix bug around pruning indices that are low causing a subtraction underflow.

## v0.0.5-alpha - 2025-01-12

### Changes

- Enable `clone`.

## v0.0.4-alpha - 2025-01-11

### Changes

- `push` has been renamed to `try_push`; `push` now only accepts a value of type `D`.

### Features

- Some extra functions added for more fine-grain control.

## v0.0.3-alpha - 2025-01-11

### Features

- Add `Debug` trait to relevant types.

## v0.0.2-alpha - 2025-01-11

### Changes

- A few functions have been renamed.
- Some documentation added/tweaked.

### Features

- `iter_along_base` added for convenience.

## v0.0.1-alpha - 2025-01-11

Initial base release.
