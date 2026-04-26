# sattvaR <img src="man/figures/logo.png" align="right" height="140"/>

<!-- badges -->
![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)
![Status](https://img.shields.io/badge/status-in--testing-yellow.svg)
![Lifecycle: Experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)
![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)

---

## Overview

`sattvaR` provides high-performance implementations of class imbalance handling methods for binary classification using the tidymodels ecosystem.

The package introduces custom recipes steps designed to:  

- Improve predictive performance in imbalanced datasets  
- Combine over- and under-sampling strategies  
- Maintain compatibility with production-grade MLOps pipelines  
- Enable auditability and reproducibility  

The first implemented method is:  

- `step_adanear()`: a hybrid method combining ADASYN and NearMiss

---

## Motivation

Class imbalance is a critical issue in real-world machine learning applications, especially in domains such as churn prediction, fraud detection, and credit risk.

This package is grounded in the framework proposed in:

Brito, João B. G. de, Bucco, Guilherme B., Heldt, Rodrigo, Becker, João. L.,  Silveira, Cleo. S., Luce, Fernando. B., Anzanello, Michel. A framework to improve churn prediction performance in retail banking. __Financial Innovation__ 10, 17 (2024). DOI [10.1186/s40854-023-00558-3](https://doi.org/10.1186/s40854-023-00558-3).

---

## Installation

```r
pak::pak("JoaoSabby/sattvaR")
```

## Quick workflow example (tidymodels)

```r
rec <- recipes::recipe(TARGET ~ ., data = dados) |>
  recipes::step_dummy(recipes::all_nominal_predictors()) |>
  recipes::update_role(ID, new_role = "id") |>
  sattvaR::step_adanear(TARGET, increaseRatio = 0.2, neighborsAdasyn = 5, neighborsNearMiss = 5, seed = 42)

wf <- workflows::workflow() |>
  workflows::add_recipe(rec) |>
  workflows::add_model(
    parsnip::boost_tree(mode = "classification", trees = 500, tree_depth = 6, learn_rate = 0.05, loss_reduction = 0, sample_size = 0.8) |>
      parsnip::set_engine("xgboost", objective = "binary:logistic", nthread = 2)
  )
```

### Common `add_recipe()` error

If you get:

- `Error in add_recipe(): ... must be empty`
- `Problematic argument: receita = ...`

use `workflows::add_recipe(rec)` (or `recipe = rec`) and **do not** use a custom argument name like `receita = ...`.


### If `library(sattvaR)` says `help/sattvaR.rdb is corrupt`

This is an installed-library issue (corrupted lazy-load/help database), not a modeling error in `step_adanear()`.

Recommended fix:

```r
# 1) unload if attached
if ("package:sattvaR" %in% search()) detach("package:sattvaR", unload = TRUE, character.only = TRUE)

# 2) remove installed copy
remove.packages("sattvaR")

# 3) install again from GitHub
pak::pak("JoaoSabby/sattvaR")

# 4) start a fresh R session and load
library(sattvaR)
```

If the error persists, manually delete the old install directory shown in the message and reinstall.

```r
rec <- recipes::recipe(TARGET ~ ., data = dados) |>
  recipes::step_dummy(recipes::all_nominal_predictors()) |>
  recipes::update_role(ID, new_role = "id") |>
  sattvaR::step_adanear(TARGET, increaseRatio = 0.2, neighborsAdasyn = 5, neighborsNearMiss = 5, seed = 42)

wf <- workflows::workflow() |>
  workflows::add_recipe(rec) |>
  workflows::add_model(
    parsnip::boost_tree(mode = "classification", trees = 500, tree_depth = 6, learn_rate = 0.05, loss_reduction = 0, sample_size = 0.8) |>
      parsnip::set_engine("xgboost", objective = "binary:logistic", nthread = 2)
  )
```

### Common `add_recipe()` error

If you get:

- `Error in add_recipe(): ... must be empty`
- `Problematic argument: receita = ...`

use `workflows::add_recipe(rec)` (or `recipe = rec`) and **do not** use a custom argument name like `receita = ...`.
