test_that("validation errors include offending predictor names", {
  training <- data.frame(
    y = factor(c("no", "no", "yes")),
    x1 = c(1, 2, 3),
    x2 = c("a", "b", "c")
  )

  expect_error(
    adanear:::ValidateNumericPredictors(training, c("x1", "x2")),
    "x2"
  )
})

test_that("adasyn feasibility error is readable", {
  expect_error(
    adanear:::ValidateAdasynFeasibility(c(no = 10L, yes = 1L), increaseRatio = 0.2),
    "pelo menos"
  )
})

test_that("prep.step_adanear learns expected state for valid recipe", {
  training <- data.frame(
    y = factor(c("no", "no", "no", "yes")),
    x1 = c(0.1, 0.2, 0.3, 0.4),
    x2 = c(1, 2, 3, 4)
  )

  rec <- recipes::recipe(y ~ ., data = training) |>
    step_adanear(y, increaseRatio = 0)

  prepped <- recipes::prep(rec, training = training)
  step <- prepped$steps[[1]]

  expect_true(step$trained)
  expect_identical(step$column, "y")
  expect_setequal(step$predictors, c("x1", "x2"))
  expect_identical(step$minorityLevel, "yes")
  expect_identical(step$majorityLevel, "no")
})

test_that("prep.step_adanear blocks non numeric predictors", {
  training <- data.frame(
    y = factor(c("no", "no", "no", "yes")),
    x1 = c(1, 2, 3, 4),
    x_bad = c("a", "b", "c", "d")
  )

  rec <- recipes::recipe(y ~ ., data = training) |>
    step_adanear(y)

  expect_error(
    recipes::prep(rec, training = training),
    "precisam ser numericos"
  )
})

test_that("step_adanear valida majorityFraction em (0, 1]", {
  training <- data.frame(
    y = factor(c("no", "no", "no", "yes")),
    x1 = c(1, 2, 3, 4)
  )

  rec <- recipes::recipe(y ~ ., data = training)

  expect_error(
    step_adanear(rec, y, majorityFraction = 0),
    "majorityFraction"
  )

  expect_no_error(
    step_adanear(rec, y, majorityFraction = 0.001)
  )
})

test_that("step_adanear valida under_ratio maior ou igual a 1", {
  training <- data.frame(
    y = factor(c("no", "no", "no", "yes")),
    x1 = c(1, 2, 3, 4)
  )

  rec <- recipes::recipe(y ~ ., data = training)

  expect_error(
    step_adanear(rec, y, under_ratio = 0.99),
    "under_ratio"
  )

  expect_no_error(
    step_adanear(rec, y, under_ratio = 1.3)
  )
})

test_that("ValidateStoredStepState falha com seed invalido", {
  obj <- list(
    trained = TRUE,
    column = "y",
    predictors = "x1",
    means = c(x1 = 0),
    sds = c(x1 = 1),
    minorityLevel = "yes",
    majorityLevel = "no",
    seed = integer(0),
    nThreads = 1L
  )

  expect_error(
    adanear:::ValidateStoredStepState(obj),
    "seed"
  )
})

test_that("ValidateStoredStepState falha com nThreads invalido", {
  obj <- list(
    trained = TRUE,
    column = "y",
    predictors = "x1",
    means = c(x1 = 0),
    sds = c(x1 = 1),
    minorityLevel = "yes",
    majorityLevel = "no",
    seed = 42L,
    nThreads = 0L
  )

  expect_error(
    adanear:::ValidateStoredStepState(obj),
    "nThreads"
  )
})

test_that("prep.step_adanear bloqueia inteiros acima de 2^53", {
  training <- data.frame(
    y = factor(c("no", "no", "no", "yes")),
    id = c(2^53 + 1, 2^53 + 3, 2^53 + 5, 2^53 + 7),
    x1 = c(1, 2, 3, 4)
  )

  rec <- recipes::recipe(y ~ ., data = training) |>
    step_adanear(y, increaseRatio = 0)

  expect_error(
    recipes::prep(rec, training = training),
    "2\\^53"
  )
})

test_that("tunable.step_adanear expõe hiperparâmetros para tune", {
  training <- data.frame(
    y = factor(c("no", "no", "no", "yes")),
    x1 = c(0.1, 0.2, 0.3, 0.4)
  )

  rec <- recipes::recipe(y ~ ., data = training) |>
    step_adanear(y, increaseRatio = 0)

  st <- rec$steps[[1]]
  tunable_tbl <- dials::tunable(st)

  expect_s3_class(tunable_tbl, "tbl_df")
  expect_setequal(
    tunable_tbl$name,
    c("increaseRatio", "neighborsAdasyn", "neighborsNearMiss", "under_ratio", "majorityFraction")
  )
})
