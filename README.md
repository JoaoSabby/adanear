# sattvaR <img src="man/figures/logo.png" align="right" height="140"/>

<!-- badges -->
![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)
![Status](https://img.shields.io/badge/status-in--testing-yellow.svg)
![Lifecycle: Experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)
![Downloads](https://img.shields.io/github/downloads/JoaoSabby/sattvaR/total?style=flat-square&color=blue)
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


