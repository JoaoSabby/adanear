test_that("validation errors include offending predictor names", {
  training <- data.frame(
    y = factor(c("no", "no", "yes")),
    x1 = c(1, 2, 3),
    x2 = c("a", "b", "c")
  )

  expect_error(
    sattvaR:::ValidateNumericPredictors(training, c("x1", "x2")),
    "x2"
  )
})

test_that("adasyn feasibility error is readable", {
  expect_error(
    sattvaR:::ValidateAdasynFeasibility(c(no = 10L, yes = 1L), increaseRatio = 0.2),
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
