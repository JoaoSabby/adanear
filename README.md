# adanear <img src="man/figures/logo.png" align="right" height="140"/>

<!-- badges -->
![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)
![Status](https://img.shields.io/badge/status-in--testing-yellow.svg)
![Lifecycle: Experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)
![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)

---

## Overview

The package introduces custom recipes steps designed to:

- Improve predictive performance in imbalanced datasets
- Combine over- and under-sampling strategies
- Stay compatible with production MLOps pipelines
- Support auditability and reproducibility

Current method:

- `step_adanear()`: a hybrid strategy that combines ADASYN and NearMiss

---

## Motivation

Class imbalance is a critical issue in real-world machine learning, especially in churn prediction, fraud detection, and credit risk.

This package is grounded in the framework proposed in:

Brito, João B. G. de, Bucco, Guilherme B., Heldt, Rodrigo, Becker, João. L., Silveira, Cleo. S., Luce, Fernando. B., Anzanello, Michel. A framework to improve churn prediction performance in retail banking. __Financial Innovation__ 10, 17 (2024). DOI [10.1186/s40854-023-00558-3](https://doi.org/10.1186/s40854-023-00558-3).

---

## Installation

```r
remotes::install_github("JoaoSabby/adanear")
```

## `step_adanear()` procedure

The balancing pipeline follows this supervised, reproducible sequence:

1. **Input validation**: the outcome must be a binary factor without missing values, and predictors must be numeric and finite.
2. **Z-score standardization**: predictors are centered and scaled using training-set means and standard deviations.
3. **ADASYN synthesis**: new minority-class samples are generated based on local neighborhood structure and `increaseRatio`.
4. **NearMiss-1 selection**: majority-class samples are selected by proximity to minority-class observations.
5. **De-standardization**: data is returned to the original scale after NearMiss.
6. **Type reconciliation**: binary and integer predictors are adjusted to preserve schema consistency.

## Binary outcome example (`modeldata`)

```r
library(recipes)
library(workflows)
library(parsnip)
library(modeldata)
library(rsample)
library(adanear)

set.seed(42)
data_tbl <- modeldata::two_class_dat

split_obj <- rsample::initial_split(data_tbl, prop = 0.8, strata = Class)
train_data <- rsample::training(split_obj)
test_data <- rsample::testing(split_obj)

rec <- recipes::recipe(Class ~ ., data = train_data) |>
  adanear::step_adanear(
    Class,
    increaseRatio = 0.2,
    under_ratio = 1L,
    neighborsAdasyn = 5L,
    neighborsNearMiss = 5L,
    nThreads = 4L,
    seed = 42L
  )

wf <- workflows::workflow() |>
  workflows::add_recipe(rec) |>
  workflows::add_model(
    parsnip::boost_tree(
      mode = "classification",
      trees = 500L,
      tree_depth = 6L,
      learn_rate = 0.05,
      loss_reduction = 0,
      sample_size = 0.8
    ) |>
      parsnip::set_engine("xgboost", objective = "binary:logistic")
  )

fit_wf <- workflows::fit(wf, data = train_data)
pred_prob <- predict(fit_wf, new_data = test_data, type = "prob")
head(pred_prob)
```

## Audit trail note (`themis::step_adasyn()` / `themis::step_nearmiss()`)

For audit purposes, `themis` balancing steps generally return only the transformed training rows during `bake()`. They do not natively expose, as dedicated metadata columns, which rows were synthetically created (`step_adasyn`) or which majority rows were removed (`step_nearmiss`).

A practical workaround is to create a stable row identifier column *before* sampling (for example with `dplyr::row_number()`), keep it through the recipe, and compare the original IDs against the baked IDs. Synthetic rows will typically have missing/new IDs depending on your implementation choices, while removed rows are those IDs absent from the baked output.

## `step_adanear()` audit example

```r
library(recipes)
library(dplyr)
library(adanear)

set.seed(42)
training <- data.frame(
  y  = factor(c(rep("no", 30), rep("yes", 6))),
  x1 = rnorm(36),
  x2 = rnorm(36)
)

rec <- recipe(y ~ ., data = training) |>
  step_adanear(
    y,
    increaseRatio = 0.5,
    under_ratio = 1,
    seed = 99L,
    nThreads = 1L,
    audit = TRUE,
    audit_id_col = ".audit_id",
    audit_origin_col = ".audit_origin"
  )

prepped <- prep(rec, training = training)
baked <- bake(prepped, new_data = training)

# Linhas sintéticas
synthetic_rows <- baked |> filter(.audit_origin == "synthetic")

# IDs originais removidos
original_ids_kept <- baked |>
  filter(.audit_origin == "original") |>
  pull(.audit_id)

removed_original_ids <- setdiff(seq_len(nrow(training)), original_ids_kept)
```
