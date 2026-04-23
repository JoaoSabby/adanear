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
