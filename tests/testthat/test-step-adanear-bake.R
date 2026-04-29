# Testes end-to-end do ciclo prep() -> bake() para step_adanear
# Cobrem: aumento correto da minoria, reproducibilidade via seed,
# invariancia de colunas e comportamento com increaseRatio = 0.

make_imbalanced_data <- function(n_majority = 20L, n_minority = 5L, seed = 1L) {
  set.seed(seed)
  data.frame(
    y  = factor(c(rep("no", n_majority), rep("yes", n_minority))),
    x1 = rnorm(n_majority + n_minority),
    x2 = rnorm(n_majority + n_minority)
  )
}

test_that("bake com increaseRatio > 0 aumenta a classe minoritaria", {
  training <- make_imbalanced_data()

  rec <- recipes::recipe(y ~ ., data = training) |>
    step_adanear(y, increaseRatio = 0.5, seed = 42L, nThreads = 1L)

  prepped <- recipes::prep(rec, training = training)
  baked   <- recipes::bake(prepped, new_data = training)

  n_minority_before <- sum(training$y == "yes")
  n_minority_after  <- sum(baked$y == "yes")

  expect_gt(n_minority_after, n_minority_before)
})

test_that("bake e reprodutivel com o mesmo seed", {
  training <- make_imbalanced_data()

  rec <- recipes::recipe(y ~ ., data = training) |>
    step_adanear(y, increaseRatio = 0.3, seed = 99L, nThreads = 1L)

  prepped <- recipes::prep(rec, training = training)
  baked1  <- recipes::bake(prepped, new_data = training)
  baked2  <- recipes::bake(prepped, new_data = training)

  expect_identical(baked1, baked2)
})

test_that("bake com increaseRatio = 0 nao altera o numero de linhas", {
  training <- make_imbalanced_data()

  rec <- recipes::recipe(y ~ ., data = training) |>
    step_adanear(y, increaseRatio = 0, seed = 7L, nThreads = 1L)

  prepped <- recipes::prep(rec, training = training)
  baked   <- recipes::bake(prepped, new_data = training)

  # Com increaseRatio = 0, nenhum sintetico e gerado e o NearMiss
  # equilibra as classes reduzindo a maioria para o tamanho da minoria.
  n_minority <- sum(training$y == "yes")
  expect_lte(nrow(baked), nrow(training))
  expect_equal(sum(baked$y == "yes"), n_minority)
  expect_equal(sum(baked$y == "no"), n_minority)
})

test_that("under_ratio controla maioria em funcao da minoria apos ADASYN", {
  training <- make_imbalanced_data(n_majority = 30L, n_minority = 6L)

  rec <- recipes::recipe(y ~ ., data = training) |>
    step_adanear(y, increaseRatio = 0.5, under_ratio = 1.3, seed = 101L, nThreads = 1L)

  prepped <- recipes::prep(rec, training = training)
  baked <- recipes::bake(prepped, new_data = training)

  n_minority_after <- sum(baked$y == "yes")
  n_majority_after <- sum(baked$y == "no")

  expect_equal(n_majority_after, ceiling(n_minority_after * 1.3))
})

test_that("bake preserva os nomes e tipos das colunas originais", {
  training <- make_imbalanced_data()

  rec <- recipes::recipe(y ~ ., data = training) |>
    step_adanear(y, increaseRatio = 0.4, seed = 11L, nThreads = 1L)

  prepped <- recipes::prep(rec, training = training)
  baked   <- recipes::bake(prepped, new_data = training)

  expect_setequal(names(baked), names(training))
  expect_true(is.factor(baked$y))
  expect_true(is.numeric(baked$x1))
  expect_true(is.numeric(baked$x2))
  expect_setequal(levels(baked$y), levels(training$y))
})

test_that("tidy retorna tibble com as configuracoes do step", {
  training <- make_imbalanced_data()

  rec <- recipes::recipe(y ~ ., data = training) |>
    step_adanear(y, increaseRatio = 0.25, seed = 55L, nThreads = 1L)

  prepped <- recipes::prep(rec, training = training)
  step    <- prepped$steps[[1]]
  td      <- recipes::tidy(step)

  expect_s3_class(td, "tbl_df")
  expect_true("terms" %in% names(td))
  expect_true("increaseRatio" %in% names(td))
  expect_true("seed" %in% names(td))
  expect_equal(td$increaseRatio, 0.25)
  expect_equal(td$seed, 55L)
})

test_that("audit = TRUE adiciona colunas de rastreabilidade", {
  training <- make_imbalanced_data(n_majority = 30L, n_minority = 6L)

  rec <- recipes::recipe(y ~ ., data = training) |>
    step_adanear(y, increaseRatio = 0.5, seed = 99L, nThreads = 1L, audit = TRUE)

  prepped <- recipes::prep(rec, training = training)
  baked <- recipes::bake(prepped, new_data = training)

  expect_true(".adanear_audit_id" %in% names(baked))
  expect_true(".adanear_audit_origin" %in% names(baked))
  expect_true(all(baked$.adanear_audit_origin %in% c("original", "synthetic")))
  expect_true(any(is.na(baked$.adanear_audit_id)))
})

test_that("seeds distintos produzem resultados potencialmente diferentes", {
  training <- make_imbalanced_data(n_majority = 30L, n_minority = 6L)

  make_baked <- function(seed_val) {
    rec <- recipes::recipe(y ~ ., data = training) |>
      step_adanear(y, increaseRatio = 0.5, seed = seed_val, nThreads = 1L)
    prepped <- recipes::prep(rec, training = training)
    recipes::bake(prepped, new_data = training)
  }

  b1 <- make_baked(42L)
  b2 <- make_baked(123L)

  # Com sementes distintas e dados suficientes, ao menos uma coluna deve diferir
  expect_false(identical(b1, b2))
})
