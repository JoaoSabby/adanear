#' ADASYN + NearMiss em um step customizado do recipes
#'
#' `step_adanear()` cria a especificacao de um step de reamostragem
#' supervisionada para classificacao binaria. O step primeiro gera exemplos
#' sinteticos da classe minoritaria com uma variacao de ADASYN e, na sequencia,
#' reduz a classe majoritaria com NearMiss-1
#'
#' A interface segue o padrao dos steps de reamostragem do pacote `themis`:
#' em `...` o usuario informa a variavel resposta usada para balanceamento
#' Os preditores sao inferidos a partir das colunas com papel de predictor no
#' recipe treinado
#'
#' O step foi desenhado para uso apenas no treino da receita. Por isso,
#' `skip = TRUE` e o comportamento recomendado
#'
#' @param recipe Um objeto `recipe`
#' @param ... Um ou mais seletores para escolher qual variavel sera usada no
#'   balanceamento. A selecao precisa resultar em uma unica variavel fator
#'   binaria. Para o metodo `tidy()`, esses seletores nao sao usados
#'   diretamente
#' @param role Nao e usado por este step porque nenhuma coluna nova e criada
#' @param trained Logico que indica se o step ja foi treinado
#' @param column String com o nome da coluna usada no balanceamento. E
#'   preenchido no `prep()`
#' @param predictors Vetor de nomes das colunas preditoras usadas nos calculos
#'   de distancia. E preenchido no `prep()`
#' @param increaseRatio Valor numerico maior ou igual a zero. Define quantas
#'   novas linhas sinteticas serao criadas em relacao ao tamanho original da
#'   minoria. Exemplo: `0.20` cria aproximadamente 20 por cento de novas linhas
#'   minoritarias antes do NearMiss
#' @param neighborsAdasyn Inteiro positivo. Numero de vizinhos usados para
#'   estimar a dificuldade local e para escolher vizinhos na interpolacao do
#'   ADASYN
#' @param neighborsNearMiss Inteiro positivo. Numero de vizinhos minoritarios
#'   usados no ranking do NearMiss-1
#' @param ef Inteiro positivo. Parametro `ef` repassado ao
#'   `RcppHNSW::hnsw_build()` e ao `RcppHNSW::hnsw_search()`
#' @param m Inteiro positivo. Parametro `M` do grafo HNSW
#' @param nThreads Inteiro positivo. Numero de threads usado nas rotinas de
#'   HNSW e RcppParallel
#' @param majorityFraction Valor numerico em `(0, 1]`. Fracao da classe
#'   majoritaria usada para estimar a dificuldade do ADASYN
#' @param seed Inteiro nao negativo. Semente local usada nas partes aleatorias
#'   do step
#' @param means Medias por coluna usadas na normalizacao. E preenchido no
#'   `prep()`
#' @param sds Desvios padrao por coluna usados na normalizacao. E preenchido no
#'   `prep()`
#' @param binaryNames Nomes de preditores binarios detectados no treino
#' @param integerNames Nomes de preditores inteiros detectados no treino,
#'   excluindo binarios
#' @param nonNegativeIntegerNames Subconjunto de `integerNames` cujos valores
#'   de treino sao sempre nao negativos
#' @param minorityLevel Nivel da classe minoritaria aprendido no `prep()`
#' @param majorityLevel Nivel da classe majoritaria aprendido no `prep()`
#' @param skip Logico. Deve permanecer `TRUE` para que o step seja aplicado
#'   apenas ao conjunto de treino
#' @param id Identificador unico do step
#'
#' @return Um objeto `recipe` com o novo step adicionado
#'
#' @details
#' Requisitos de uso:
#' \itemize{
#'   \item a variavel selecionada em `...` precisa ser um fator binario sem `NA`;
#'   \item os preditores do recipe precisam ser numericos, finitos e sem `NA`;
#'   \item o step deve vir depois de imputacao, tratamento de categorias raras e
#'   dummy encoding, quando isso for necessario;
#'   \item o treino precisa ter desbalanceamento real. Se as classes estiverem
#'   empatadas, o step falha de forma explicita
#' }
#'
#' Todos os preditores usados nos calculos de distancia sao inferidos a partir
#' das colunas com papel de predictor no recipe. Todas as colunas do conjunto
#' de dados sao devolvidas por `bake()`
#'
#' @examples
#' \dontrun{
#' recipes::recipe(target ~ ., data = dadosTreino) |>
#'   recipes::step_impute_median(recipes::all_numeric_predictors()) |>
#'   recipes::step_unknown(recipes::all_nominal_predictors()) |>
#'   recipes::step_other(recipes::all_nominal_predictors(), threshold = 0.01) |>
#'   recipes::step_dummy(recipes::all_nominal_predictors(), one_hot = TRUE) |>
#'   recipes::step_nzv(recipes::all_predictors()) |>
#'   step_adanear(
#'     target,
#'     increaseRatio = 0.20,
#'     neighborsAdasyn = 5L,
#'     neighborsNearMiss = 5L,
#'     nThreads = 8L,
#'     majorityFraction = 0.20,
#'     seed = 42L
#'   )
#' }
#'
DefaultThreadCount <- function() {
  detectedCores <- as.integer(parallel::detectCores() / 2L)

  if (length(detectedCores) != 1L || is.na(detectedCores) || detectedCores < 2L) {
    return(1L)
  }

  as.integer(detectedCores - 1L)
}

step_adanear <- function(recipe,
                         ...,
                         role = NA,
                         trained = FALSE,
                         column = NULL,
                         predictors = NULL,
                         increaseRatio = 0.20,
                         neighborsAdasyn = 5L,
                         neighborsNearMiss = 5L,
                         ef = 200L,
                         m = 16L,
                         nThreads = DefaultThreadCount(),
                         majorityFraction = 1.0,
                         seed = sample.int(10^5, 1),
                         means = NULL,
                         sds = NULL,
                         binaryNames = NULL,
                         integerNames = NULL,
                         nonNegativeIntegerNames = NULL,
                         minorityLevel = NULL,
                         majorityLevel = NULL,
                         skip = TRUE,
                         id = recipes::rand_id("adanear")) {

  recipes::recipes_pkg_check(required_pkgs.step_adanear(NULL))

  ValidateSingleNumber(increaseRatio, "increaseRatio", minValue = 0)
  ValidateSingleNumber(neighborsAdasyn, "neighborsAdasyn", minValue = 1, integerish = TRUE)
  ValidateSingleNumber(neighborsNearMiss, "neighborsNearMiss", minValue = 1, integerish = TRUE)
  ValidateSingleNumber(ef, "ef", minValue = 1, integerish = TRUE)
  ValidateSingleNumber(m, "m", minValue = 1, integerish = TRUE)
  ValidateSingleNumber(nThreads, "nThreads", minValue = 1, integerish = TRUE)
  ValidateSingleNumber(majorityFraction, "majorityFraction", minValue = 0.01, maxValue = 1)

  if (!is.null(seed)) {
    ValidateSingleNumber(seed, "seed", minValue = 0, integerish = TRUE)
  }

  recipes::add_step(
    recipe,
    recipes::step(
      subclass = "adanear",
      terms = rlang::enquos(...),
      role = role,
      trained = trained,
      column = column,
      predictors = predictors,
      increaseRatio = increaseRatio,
      neighborsAdasyn = as.integer(neighborsAdasyn),
      neighborsNearMiss = as.integer(neighborsNearMiss),
      ef = as.integer(ef),
      m = as.integer(m),
      nThreads = as.integer(max(1L, nThreads)),
      majorityFraction = majorityFraction,
      seed = as.integer(seed),
      means = means,
      sds = sds,
      binaryNames = binaryNames,
      integerNames = integerNames,
      nonNegativeIntegerNames = nonNegativeIntegerNames,
      minorityLevel = minorityLevel,
      majorityLevel = majorityLevel,
      skip = skip,
      id = id
    )
  )
}

minMinoritySize <- 2L
integerTolerance <- 1e-8

#' Treina o step_adanear
#'
#' Resolve a coluna alvo selecionada em `...`, identifica os preditores do
#' recipe, valida o contrato do step e armazena tudo que sera necessario para a
#' reamostragem no `bake()`
#'
#' @param x Um objeto `step_adanear`
#' @param training Dados de treino recebidos pelo `prep()`
#' @param info Metadados do recipe
#' @param ... Nao usado
#'
#' @return Um objeto `step_adanear` treinado
#'
#' @rdname step_adanear
#' @export
prep.step_adanear <- function(x, training, info = NULL, ...) {
  selectedColumn <- ResolveSamplingColumn(x$terms, training, info)
  ValidateOutcomeRole(selectedColumn, info)

  predictorNames <- ResolvePredictorNames(selectedColumn, info)

  ValidateOutcome(training, selectedColumn)
  ValidateNumericPredictors(training, predictorNames)

  xMatrix <- as.matrix(training[, predictorNames, drop = FALSE])
  normStats <- BuildNormalizationStats(xMatrix)
  typeNames <- DetectPredictorTypes(training, predictorNames)
  classTable <- table(training[[selectedColumn]])
  classLevels <- DetectClassLevels(training[[selectedColumn]])
  ValidateAdasynFeasibility(classTable, x$increaseRatio)

  recipes::step(
    subclass = "adanear",
    terms = x$terms,
    role = x$role,
    trained = TRUE,
    column = selectedColumn,
    predictors = predictorNames,
    increaseRatio = x$increaseRatio,
    neighborsAdasyn = x$neighborsAdasyn,
    neighborsNearMiss = x$neighborsNearMiss,
    ef = x$ef,
    m = x$m,
    nThreads = x$nThreads,
    majorityFraction = x$majorityFraction,
    seed = x$seed,
    means = normStats$means,
    sds = normStats$sds,
    binaryNames = typeNames$binaryNames,
    integerNames = typeNames$integerNames,
    nonNegativeIntegerNames = typeNames$nonNegativeIntegerNames,
    minorityLevel = classLevels$minority,
    majorityLevel = classLevels$majority,
    skip = x$skip,
    id = x$id
  )
}

#' Aplica o step_adanear
#'
#' Quando aplicado ao conjunto de treino, o step cria sinteticos com ADASYN,
#' concatena esses dados ao treino original e depois aplica NearMiss-1 para
#' equilibrar as classes. Em novos dados, o comportamento esperado e que o step
#' seja pulado quando `skip = TRUE`
#'
#' @param object Um objeto `step_adanear` treinado
#' @param new_data Dados que serao transformados
#' @param ... Nao usado
#'
#' @return Um tibble com os dados reamostrados
#'
#' @rdname step_adanear
#' @export
bake.step_adanear <- function(object, new_data, ...) {
  recipes::check_new_data(
    c(object$predictors, object$column),
    object,
    new_data
  )

  ValidateOutcome(new_data, object$column)
  ValidateNumericPredictors(new_data, object$predictors)
  ValidateStoredStepState(object)

  ExecuteBake <- function() {
    xNorm <- NormalizeMatrixCpp(
      x = as.matrix(new_data[, object$predictors, drop = FALSE]),
      means = object$means,
      sds = object$sds,
      numThreads = as.integer(max(1L, object$nThreads))
    )

    syntheticData <- GenerateAdasynRows(
      dataFrame = new_data,
      xNorm = xNorm,
      predictorNames = object$predictors,
      outcomeName = object$column,
      means = object$means,
      sds = object$sds,
      minorityLevel = object$minorityLevel,
      increaseRatio = object$increaseRatio,
      binaryNames = object$binaryNames,
      integerNames = object$integerNames,
      nonNegativeIntegerNames = object$nonNegativeIntegerNames,
      neighborsAdasyn = object$neighborsAdasyn,
      ef = object$ef,
      m = object$m,
      nThreads = object$nThreads,
      majorityFraction = object$majorityFraction
    )

    augmentedData <- data.table::rbindlist(
      list(
        data.table::as.data.table(new_data),
        data.table::as.data.table(syntheticData)
      ),
      use.names = TRUE
    )

    xNormAug <- NormalizeMatrixCpp(
      x = as.matrix(augmentedData[, object$predictors, with = FALSE]),
      means = object$means,
      sds = object$sds,
      numThreads = as.integer(max(1L, object$nThreads))
    )

    balancedData <- SelectNearMissRows(
      dataFrame = augmentedData,
      xNorm = xNormAug,
      outcomeName = object$column,
      minorityLevel = object$minorityLevel,
      majorityLevel = object$majorityLevel,
      neighborsNearMiss = object$neighborsNearMiss,
      ef = object$ef,
      m = object$m,
      nThreads = object$nThreads
    )

    balancedData <- balancedData[, colnames(new_data), with = FALSE]
    balancedData <- RestoreOriginalColumnTypes(balancedData, new_data)
    tibble::as_tibble(balancedData)
  }

  withr::with_seed(object$seed, ExecuteBake())
}

#' Imprime um resumo do step_adanear
#'
#' @param x Um objeto `step_adanear`
#' @param width Largura usada pelo printer do recipes
#' @param ... Nao usado
#'
#' @return O proprio objeto, invisivelmente
#'
#' @rdname step_adanear
#' @export
print.step_adanear <- function(x, width = max(20, getOption("width") - 30), ...) {
  termLabel <- if (recipes::is_trained(x)) x$column else recipes::sel2char(x$terms)

  cat("ADASYN + NearMiss sampling on ")
  recipes::printer(termLabel, x$terms, x$trained, width = width)
  cat(
    sprintf(
      "  increaseRatio: %.3f | neighborsAdasyn: %d | neighborsNearMiss: %d | ef: %d | M: %d | threads: %d | majFrac:
%.2f | seed: %d\n",
      x$increaseRatio,
      x$neighborsAdasyn,
      x$neighborsNearMiss,
      x$ef,
      x$m,
      x$nThreads,
      x$majorityFraction,
      x$seed
    )
  )
  invisible(x)
}

#' Gera um tibble descritivo do step_adanear
#'
#' @param x Um objeto `step_adanear`
#' @param ... Nao usado
#'
#' @return Um tibble com a configuracao principal do step
#'
#' @rdname step_adanear
#' @export
tidy.step_adanear <- function(x, ...) {
  termsValues <- if (recipes::is_trained(x)) x$column else recipes::sel2char(x$terms)

  tibble::tibble(
    terms = termsValues,
    increaseRatio = x$increaseRatio,
    neighborsAdasyn = x$neighborsAdasyn,
    neighborsNearMiss = x$neighborsNearMiss,
    ef = x$ef,
    m = x$m,
    nThreads = x$nThreads,
    majorityFraction = x$majorityFraction,
    seed = x$seed,
    id = x$id
  )
}

#' Lista pacotes necessarios para o step_adanear
#'
#' @param x Um objeto `step_adanear`
#' @param ... Nao usado
#'
#' @return Um vetor de nomes de pacotes
#'
#' @rdname step_adanear
#' @export
required_pkgs.step_adanear <- function(x, ...) {
  c(
    "recipes",
    "rlang",
    "tibble",
    "data.table",
    "RcppHNSW",
    "RcppParallel",
    "parallel",
    "withr",
    "stringr"
  )
}

#' Resolve a coluna usada no balanceamento
#'
#' @param terms Seletores salvos no step
#' @param training Dados de treino
#' @param info Metadados do recipe
#'
#' @return String com o nome da coluna selecionada
#'
#' @keywords internal
ResolveSamplingColumn <- function(terms, training, info) {
  selected <- recipes::recipes_eval_select(terms, training, info)
  selectedNames <- names(selected)

  if (length(selectedNames) == 0L) {
    selectedNames <- colnames(training)[unname(selected)]
  }

  if (length(selectedNames) != 1L) {
    rlang::abort("step_adanear() requer que `...` selecione exatamente uma coluna fator binaria.")
  }

  selectedNames[[1]]
}

#' Valida se a coluna selecionada e a resposta do recipe
#'
#' @param column Nome da coluna selecionada
#' @param info Metadados do recipe
#'
#' @return Invisivelmente, `TRUE` quando a validacao passa
#'
#' @keywords internal
ValidateOutcomeRole <- function(column, info) {
  outcomeNames <- info$variable[info$role == "outcome"]

  if (!(column %in% outcomeNames)) {
    rlang::abort("step_adanear() requer que `...` selecione a variavel resposta do recipe.")
  }

  invisible(TRUE)
}

#' Resolve os preditores do recipe
#'
#' @param column Nome da coluna resposta
#' @param info Metadados do recipe
#'
#' @return Vetor de nomes dos preditores
#'
#' @keywords internal
ResolvePredictorNames <- function(column, info) {
  predictorNames <- info$variable[info$role == "predictor"]
  predictorNames <- setdiff(predictorNames, column)

  if (length(predictorNames) == 0L) {
    rlang::abort("step_adanear() requer pelo menos um preditor com papel de predictor no recipe.")
  }

  predictorNames
}

#' Valida a variavel resposta
#'
#' @param training Data frame usado na validacao
#' @param outcomeName Nome da coluna de resposta
#'
#' @return Invisivelmente, `TRUE` quando a validacao passa
#'
#' @keywords internal
ValidateOutcome <- function(training, outcomeName) {
  yVector <- training[[outcomeName]]

  if (!is.factor(yVector)) {
    rlang::abort("A variavel selecionada em `...` precisa ser um fator binario.")
  }

  if (anyNA(yVector)) {
    rlang::abort("A variavel resposta nao pode conter NA.")
  }

  if (nlevels(yVector) != 2L) {
    rlang::abort("A variavel resposta precisa ter exatamente dois niveis.")
  }

  classTable <- table(yVector)

  if (any(classTable <= 0L)) {
    rlang::abort("As duas classes precisam estar presentes no treino.")
  }

  if (length(unique(as.integer(classTable))) == 1L) {
    rlang::abort("As classes estao empatadas no treino. step_adanear() exige desbalanceamento real.")
  }

  invisible(TRUE)
}

#' Valida o conjunto de preditores
#'
#' @param training Data frame usado na validacao
#' @param predictorNames Nomes dos preditores
#'
#' @return Invisivelmente, `TRUE` quando a validacao passa
#'
#' @keywords internal
ValidateNumericPredictors <- function(training, predictorNames) {
  if (length(predictorNames) == 0L) {
    rlang::abort("step_adanear() requer pelo menos um preditor.")
  }

  predictorFrame <- training[, predictorNames, drop = FALSE]

  isNumeric <- vapply(predictorFrame, is.numeric, logical(1))
  if (!all(isNumeric)) {
    badNames <- names(isNumeric)[!isNumeric]
    rlang::abort(
      stringr::str_sub(
        "Todos os preditores precisam ser numericos. Problemas em: ",
        stringr::str_sub(badNames, collapse = ", "),
        ". Aplique imputacao, encoding e demais passos antes de step_adanear()."
      )
    )
  }

  hasMissing <- vapply(predictorFrame, anyNA, logical(1))
  if (any(hasMissing)) {
    badNames <- names(hasMissing)[hasMissing]
    rlang::abort(
      stringr::str_sub(
        "Todos os preditores precisam estar sem NA antes de step_adanear(). Problemas em: ",
        stringr::str_sub(badNames, collapse = ", ")
      )
    )
  }

  isFinite <- vapply(
    predictorFrame,
    function(column) all(is.finite(column)),
    logical(1)
  )

  if (!all(isFinite)) {
    badNames <- names(isFinite)[!isFinite]
    rlang::abort(
      stringr::str_sub(
        "Todos os preditores precisam conter apenas valores finitos. Problemas em: ",
        stringr::str_sub(badNames, collapse = ", ")
      )
    )
  }

  invisible(TRUE)
}

#' Valida os artefatos aprendidos no prep
#'
#' @param object Um objeto `step_adanear` treinado
#'
#' @return Invisivelmente, `TRUE` quando os artefatos internos estao coerentes
#'
#' @keywords internal
ValidateStoredStepState <- function(object) {
  if (!isTRUE(object$trained)) {
    rlang::abort("step_adanear() precisa estar treinado antes do bake().")
  }

  if (length(object$column) != 1L) {
    rlang::abort("O step treinado nao possui a coluna resposta armazenada corretamente.")
  }

  if (length(object$predictors) == 0L) {
    rlang::abort("O step treinado nao possui preditores armazenados.")
  }

  if (is.null(object$means) || is.null(object$sds)) {
    rlang::abort("O step treinado nao possui estatisticas de normalizacao.")
  }

  if (length(object$means) != length(object$predictors)) {
    rlang::abort("As medias armazenadas nao batem com a quantidade de preditores.")
  }

  if (length(object$sds) != length(object$predictors)) {
    rlang::abort("Os desvios armazenados nao batem com a quantidade de preditores.")
  }

  if (any(!is.finite(object$means))) {
    rlang::abort("As medias armazenadas precisam ser finitas.")
  }

  if (any(!is.finite(object$sds)) || any(object$sds <= 0)) {
    rlang::abort("Os desvios armazenados precisam ser finitos e positivos.")
  }

  if (is.null(object$minorityLevel) || is.null(object$majorityLevel)) {
    rlang::abort("O step treinado nao possui niveis de classe armazenados.")
  }

  invisible(TRUE)
}

#' Valida se a parte ADASYN e viavel no treino
#'
#' @param classTable Tabela de frequencia da resposta
#' @param increaseRatio Razao relativa de novas linhas
#'
#' @return Invisivelmente, `TRUE` quando a configuracao e viavel
#'
#' @keywords internal
ValidateAdasynFeasibility <- function(classTable, increaseRatio) {
  if (increaseRatio <= 0) {
    return(invisible(TRUE))
  }

  minorityCount <- min(as.integer(classTable))

  if (minorityCount < minMinoritySize) {
    rlang::abort(
      stringr::str_sub(
        "Com `increaseRatio > 0`, a classe minoritaria precisa ter pelo menos ",
        minMinoritySize,
        " observacoes."
      )
    )
  }

  invisible(TRUE)
}

#' Calcula medias e desvios da normalizacao
#'
#' @param xMatrix Matriz numerica dos preditores
#'
#' @return Lista com `means` e `sds`
#'
#' @keywords internal
BuildNormalizationStats <- function(xMatrix) {
  if (!is.matrix(xMatrix)) {
    rlang::abort("BuildNormalizationStats() espera uma matriz.")
  }

  if (ncol(xMatrix) == 0L) {
    rlang::abort("BuildNormalizationStats() recebeu zero colunas.")
  }

  means <- colMeans(xMatrix)
  sds <- vapply(seq_len(ncol(xMatrix)), function(colIdx) stats::sd(xMatrix[, colIdx]), numeric(1))
  names(sds) <- colnames(xMatrix)
  sds[is.na(sds) | sds == 0] <- 1.0

  list(
    means = means,
    sds = sds
  )
}

#' Detecta os niveis majoritario e minoritario
#'
#' @param yVector Vetor fator da resposta
#'
#' @return Lista com `minority` e `majority`
#'
#' @keywords internal
DetectClassLevels <- function(yVector) {
  classTable <- table(yVector)

  if (length(unique(as.integer(classTable))) == 1L) {
    rlang::abort("As classes estao empatadas. Nao ha minoria e maioria bem definidas.")
  }

  list(
    minority = names(classTable)[which.min(classTable)],
    majority = names(classTable)[which.max(classTable)]
  )
}

#' Detecta colunas binarias e inteiras
#'
#' @param training Data frame de treino
#' @param predictorNames Nomes dos preditores
#'
#' @return Lista com `binaryNames`, `integerNames` e `nonNegativeIntegerNames`
#'
#' @keywords internal
DetectPredictorTypes <- function(training, predictorNames) {
  predictorFrame <- training[, predictorNames, drop = FALSE]

  binaryNames <- predictorNames[
    vapply(predictorFrame, IsBinaryNumeric, logical(1))
  ]

  integerNames <- setdiff(
    predictorNames[
      vapply(predictorFrame, IsWholeNumber, logical(1))
    ],
    binaryNames
  )

  nonNegativeIntegerNames <- if (length(integerNames) == 0L) {
    character(0)
  } else {
    integerNames[
      vapply(
        training[, integerNames, drop = FALSE],
        function(column) all(column >= 0),
        logical(1)
      )
    ]
  }

  list(
    binaryNames = binaryNames,
    integerNames = integerNames,
    nonNegativeIntegerNames = nonNegativeIntegerNames
  )
}

#' Verifica se um vetor numerico representa inteiros
#'
#' @param x Vetor numerico
#' @param tolerance Tolerancia numerica para arredondamento
#'
#' @return `TRUE` quando todos os valores sao inteiros, desconsiderando `NA`
#'
#' @keywords internal
IsWholeNumber <- function(x, tolerance = integerTolerance) {
  if (!is.numeric(x)) {
    return(FALSE)
  }

  values <- x[!is.na(x)]

  if (length(values) == 0L) {
    return(FALSE)
  }

  all(abs(values - round(values)) < tolerance)
}

#' Verifica se um vetor numerico representa variavel binaria
#'
#' @param x Vetor numerico
#' @param tolerance Tolerancia numerica para comparar com 0 e 1
#'
#' @return `TRUE` quando todos os valores sao 0 ou 1, desconsiderando `NA`
#'
#' @keywords internal
IsBinaryNumeric <- function(x, tolerance = integerTolerance) {
  if (!is.numeric(x)) {
    return(FALSE)
  }

  values <- x[!is.na(x)]

  if (length(values) == 0L) {
    return(FALSE)
  }

  all(abs(values) < tolerance | abs(values - 1.0) < tolerance)
}

#' Construi o indice HNSW
#'
#' Funcao utilitaria que monta o indice aproximado de vizinhos mais proximos
#' usando `RcppHNSW::hnsw_build()`. Cada linha da matriz representa uma
#' observacao e cada coluna representa um preditor numerico
#'
#' @param xMatrix Matriz numerica usada para indexacao
#' @param ef Inteiro positivo. Tamanho da lista dinamica usada na construcao
#' @param m Inteiro positivo. Numero de ligacoes bidirecionais por elemento no
#'   grafo HNSW
#' @param nThreads Inteiro positivo. Numero maximo de threads
#'
#' @return Um objeto de indice retornado por `RcppHNSW::hnsw_build()`
#'
#' @keywords internal
BuildHnswIndex <- function(xMatrix, ef, m, nThreads) {
  if (!is.matrix(xMatrix)) {
    rlang::abort("BuildHnswIndex() espera uma matriz.")
  }

  if (!is.numeric(xMatrix)) {
    rlang::abort("BuildHnswIndex() espera uma matriz numerica.")
  }

  if (nrow(xMatrix) <= 0L) {
    rlang::abort("BuildHnswIndex() recebeu zero linhas.")
  }

  if (ncol(xMatrix) <= 0L) {
    rlang::abort("BuildHnswIndex() recebeu zero colunas.")
  }

  if (anyNA(xMatrix) || any(!is.finite(xMatrix))) {
    rlang::abort("BuildHnswIndex() recebeu valores NA, NaN ou Inf.")
  }

  RcppHNSW::hnsw_build(
    X = xMatrix,
    distance = "euclidean",
    M = as.integer(m),
    ef = as.integer(ef),
    n_threads = as.integer(max(1L, nThreads)),
    grain_size = max(1000L, as.integer(nrow(xMatrix) / (nThreads * 4L))),
    byrow = TRUE,
    verbose = FALSE
  )
}

#' Executa uma consulta HNSW e normaliza a saida
#'
#' @param index Objeto retornado por `BuildHnswIndex()`
#' @param queryMatrix Matriz de consulta
#' @param k Quantidade de vizinhos desejada
#' @param ef Parametro `ef` da busca
#' @param nThreads Numero de threads
#'
#' @return Matriz inteira `nConsulta x k` com indices 1-based
#'
#' @keywords internal
QueryHnswIndex <- function(index, queryMatrix, neighbors, ef, nThreads){
  if (!is.matrix(queryMatrix)) {
    rlang::abort("QueryHnswIndex() espera uma matriz de consulta.")
  }

  if (!is.numeric(queryMatrix)) {
    rlang::abort("QueryHnswIndex() espera uma matriz numerica.")
  }

  if (nrow(queryMatrix) == 0L) {
    rlang::abort("QueryHnswIndex() recebeu zero linhas de consulta.")
  }

  if (ncol(queryMatrix) == 0L) {
    rlang::abort("QueryHnswIndex() recebeu zero colunas de consulta.")
  }

  if (anyNA(queryMatrix) || any(!is.finite(queryMatrix))) {
    rlang::abort("QueryHnswIndex() recebeu valores NA, NaN ou Inf.")
  }

  ValidateSingleNumber(neighbors , "neighbors", minValue = 1L, integerish = TRUE)
  ValidateSingleNumber(ef, "ef", minValue = 1L, integerish = TRUE)
  ValidateSingleNumber(nThreads, "nThreads", minValue = 1L, integerish = TRUE)

  searchResult <- RcppHNSW::hnsw_search(
    X = queryMatrix,
    ann = index,
    k = as.integer(neighbors),
    ef = as.integer(ef),
    n_threads = as.integer(max(1L, nThreads)),
    grain_size = max(1000L, as.integer(nrow(queryMatrix) / (nThreads * 4L))),
    byrow = TRUE
  )

  idx <- searchResult$idx

  if (is.null(idx)) {
    rlang::abort("RcppHNSW::hnsw_search() nao retornou indices.")
  }

  if (is.null(dim(idx))) {
    idx <- matrix(as.integer(idx), nrow = nrow(queryMatrix), byrow = TRUE)
  }

  if (!identical(nrow(idx), nrow(queryMatrix))) {
    rlang::abort("A matriz de indices retornada pelo HNSW veio com numero inesperado de linhas.")
  }

  storage.mode(idx) <- "integer"
  idx
}

#' Distribui quantos sinteticos cada ancora vai gerar
#'
#' @param weightVector Pesos de dificuldade por observacao minoritaria
#' @param totalNew Quantidade total de sinteticos desejada
#'
#' @return Vetor inteiro com a quantidade alocada por ancora
#'
#' @keywords internal
AllocateSyntheticCounts <- function(weightVector, totalNew) {

  if (totalNew <= 0L || length(weightVector) == 0L) {
    return(integer(length(weightVector)))
  }

  if (any(!is.finite(weightVector)) || any(weightVector < 0)) {
    rlang::abort("AllocateSyntheticCounts() recebeu pesos invalidos.")
  }

  normalizedWeights <- if (sum(weightVector) <= 0) {
    rep(1 / length(weightVector), length(weightVector))
  } else {
    weightVector / sum(weightVector)
  }

  rawCounts <- normalizedWeights * totalNew
  intCounts <- floor(rawCounts)
  remainder <- as.integer(totalNew - sum(intCounts))

  if (remainder > 0L) {
    topIdx <- order(rawCounts - intCounts, decreasing = TRUE)
    fillIdx <- topIdx[seq_len(remainder)]
    intCounts[fillIdx] <- intCounts[fillIdx] + 1L
  }

  as.integer(intCounts)
}

#' Calcula a dificuldade local do ADASYN
#'
#' @param xNorm Matriz normalizada com todos os preditores
#' @param yVector Vetor fator da resposta
#' @param minorityLevel Nivel minoritario
#' @param neighborsAdasyn Numero de vizinhos do ADASYN
#' @param ef Parametro `ef`
#' @param m Parametro `M`
#' @param nThreads Numero de threads
#' @param majorityFraction Fracao da maioria usada na estimacao da dificuldade
#'
#' @return Vetor numerico com uma dificuldade por linha minoritaria
#'
#' @keywords internal
GetAdasynDifficulty <- function(xNorm,
                                yVector,
                                minorityLevel,
                                neighborsAdasyn,
                                ef,
                                m,
                                nThreads,
                                majorityFraction) {
  minorityIdx <- which(yVector == minorityLevel)
  majorityIdx <- which(yVector != minorityLevel)

  if (length(minorityIdx) < minMinoritySize) {
    rlang::abort(
      stringr::str_sub(
        "A classe minoritaria precisa ter pelo menos ",
        minMinoritySize,
        " observacoes."
      )
    )
  }

  sampledMajorityIdx <- if (majorityFraction < 1.0) {
    sampleSize <- ceiling(length(majorityIdx) * majorityFraction)
    sampleSize <- max(1L, min(length(majorityIdx), sampleSize))
    sample(majorityIdx, size = sampleSize, replace = FALSE)
  } else {
    majorityIdx
  }

  mixedIdx <- c(minorityIdx, sampledMajorityIdx)
  xMixed <- xNorm[mixedIdx, , drop = FALSE]
  yMixed <- yVector[mixedIdx]
  indexMixed <- BuildHnswIndex(xMixed, ef, m, nThreads)
  neighborsEffective <- min(neighborsAdasyn + 1L, nrow(xMixed))

  knnIdx <- QueryHnswIndex(
    index = indexMixed,
    queryMatrix = xMixed[seq_along(minorityIdx), , drop = FALSE],
    neighbors = neighborsEffective,
    ef = ef,
    nThreads = nThreads
  )

  yBinary <- as.integer(yMixed != minorityLevel)
  minorityMixedIdx <- as.integer(seq_along(minorityIdx))

  ComputeDifficultyCpp(
    knnIdx = knnIdx,
    minGlobalIdx = minorityMixedIdx,
    yBinary = yBinary,
    numThreads = as.integer(max(1L, nThreads))
  )
}

#' Expande o plano ancora-vizinho dos sinteticos
#'
#' @param synCounts Quantos sinteticos cada ancora deve gerar
#' @param knnLocal Matriz de vizinhos locais da minoria
#' @param nMinority Numero de linhas da minoria
#'
#' @return Lista com vetores `anchor` e `neighbor`
#'
#' @keywords internal
ExpandAnchorNeighborPairs <- function(synCounts, knnLocal, nMinority) {
  anchorLocal <- rep(seq_len(nMinority), times = synCounts)

  if (length(anchorLocal) == 0L) {
    return(list(anchor = integer(0), neighbor = integer(0)))
  }

  neighborLocal <- SampleNeighborsCpp(knnLocal, anchorLocal)
  validMask <- !is.na(neighborLocal)

  list(
    anchor = anchorLocal[validMask],
    neighbor = neighborLocal[validMask]
  )
}

#' Resolve nomes de colunas em indices 0-based
#'
#' @param targetNames Nomes que devem ser encontrados
#' @param predictorNames Vetor base de nomes dos preditores
#'
#' @return Vetor inteiro 0-based
#'
#' @keywords internal
ResolveColumnIndices <- function(targetNames, predictorNames) {
  if (length(targetNames) == 0L) {
    return(integer(0))
  }

  idx <- match(targetNames, predictorNames)

  if (anyNA(idx)) {
    missingNames <- targetNames[is.na(idx)]
    rlang::abort(
      stringr::str_sub(
        "Falha ao mapear colunas para restauracao de tipos. Problemas em: ",
        stringr::str_sub(missingNames, collapse = ", ")
      )
    )
  }

  as.integer(idx - 1L)
}

#' Interpola sinteticos no espaco normalizado e restaura tipos
#'
#' @param minMat Matriz normalizada contendo apenas a minoria
#' @param anchorLocal Vetor 1-based de ancoras
#' @param neighborLocal Vetor 1-based de vizinhos escolhidos
#' @param means Medias da normalizacao
#' @param sds Desvios da normalizacao
#' @param binaryNames Nomes de colunas binarias
#' @param integerNames Nomes de colunas inteiras
#' @param nonNegativeIntegerNames Nomes de colunas inteiras nao negativas
#' @param predictorNames Nomes de todos os preditores
#'
#' @return Matriz numerica com os sinteticos na escala original
#'
#' @keywords internal
InterpolateSyntheticPoints <- function(
    minMat,
    anchorLocal,
    neighborLocal,
    means,
    sds,
    binaryNames,
    integerNames,
    nonNegativeIntegerNames,
    predictorNames,
    nThreads
){

  lambdas <- stats::runif(length(anchorLocal))

  synNorm <- AdasynInterpolateCpp(
    minMat = minMat,
    anchorIdx = as.integer(anchorLocal - 1L),
    neighborIdx = as.integer(neighborLocal - 1L),
    lambdas = lambdas,
    numThreads = as.integer(max(1L, nThreads))
  )

  synRaw <- DenormalizeMatrixCpp(
    x = synNorm,
    means = means,
    sds = sds,
    numThreads = as.integer(max(1L, nThreads))
  )

  RestoreTypesCpp(
    synMat = synRaw,
    binCols = ResolveColumnIndices(binaryNames, predictorNames),
    intCols = ResolveColumnIndices(integerNames, predictorNames),
    nnIntCols = ResolveColumnIndices(nonNegativeIntegerNames, predictorNames),
    numThreads = as.integer(max(1L, nThreads))
  )

  colnames(synRaw) <- predictorNames
  synRaw
}

#' Construi o data frame final dos sinteticos
#'
#' @param dataFrame Dados base usados como molde
#' @param synRaw Matriz com os sinteticos ja restaurados
#' @param minorityIdx Indices absolutos da minoria no data frame base
#' @param anchorLocal Vetor 1-based com as ancoras usadas na geracao
#' @param predictorNames Nomes dos preditores
#' @param outcomeName Nome da resposta
#' @param minorityLevel Nivel da classe minoritaria
#'
#' @return Um tibble contendo apenas as linhas sinteticas
#'
#' @keywords internal
BuildSyntheticDataFrame <- function(dataFrame,
                                    synRaw,
                                    minorityIdx,
                                    anchorLocal,
                                    predictorNames,
                                    outcomeName,
                                    minorityLevel) {
  syntheticTable <- data.table::as.data.table(
    dataFrame[minorityIdx[anchorLocal], , drop = FALSE]
  )

  data.table::set(
    syntheticTable,
    j = predictorNames,
    value = as.data.frame(synRaw, stringsAsFactors = FALSE)
  )

  data.table::set(
    syntheticTable,
    j = outcomeName,
    value = factor(
      rep(minorityLevel, nrow(syntheticTable)),
      levels = levels(dataFrame[[outcomeName]])
    )
  )

  tibble::as_tibble(syntheticTable)
}

#' Gera linhas sinteticas com ADASYN
#'
#' @param dataFrame Dados de entrada
#' @param xNorm Matriz normalizada dos preditores
#' @param predictorNames Nomes dos preditores
#' @param outcomeName Nome da resposta
#' @param means Medias da normalizacao
#' @param sds Desvios da normalizacao
#' @param minorityLevel Nivel minoritario
#' @param increaseRatio Razao relativa de novas linhas
#' @param binaryNames Nomes de colunas binarias
#' @param integerNames Nomes de colunas inteiras
#' @param nonNegativeIntegerNames Nomes de colunas inteiras nao negativas
#' @param neighborsAdasyn Numero de vizinhos do ADASYN
#' @param ef Parametro `ef`
#' @param m Parametro `M`
#' @param nThreads Numero de threads
#' @param majorityFraction Fracao da maioria usada no calculo da dificuldade
#'
#' @return Um data frame com apenas as linhas sinteticas
#'
#' @keywords internal
GenerateAdasynRows <- function(dataFrame,
                               xNorm,
                               predictorNames,
                               outcomeName,
                               means,
                               sds,
                               minorityLevel,
                               increaseRatio,
                               binaryNames,
                               integerNames,
                               nonNegativeIntegerNames,
                               neighborsAdasyn,
                               ef,
                               m,
                               nThreads,
                               majorityFraction) {
  yVector <- dataFrame[[outcomeName]]
  minorityIdx <- which(yVector == minorityLevel)
  nMinority <- length(minorityIdx)
  nNew <- as.integer(round(nMinority * increaseRatio))

  if (nNew <= 0L) {
    return(dataFrame[0L, , drop = FALSE])
  }

  if (nMinority < minMinoritySize) {
    rlang::abort(stringr::str_sub("ADASYN requer pelo menos ", minMinoritySize, " observacoes minoritarias."))
  }

  difficulty <- GetAdasynDifficulty(
    xNorm = xNorm,
    yVector = yVector,
    minorityLevel = minorityLevel,
    neighborsAdasyn = neighborsAdasyn,
    ef = ef,
    m = m,
    nThreads = nThreads,
    majorityFraction = majorityFraction
  )

  synCounts <- AllocateSyntheticCounts(difficulty, nNew)
  minMat <- xNorm[minorityIdx, , drop = FALSE]
  indexMin <- BuildHnswIndex(minMat, ef, m, nThreads)
  neighborsEffective <- min(neighborsAdasyn + 1L, nrow(minMat))

  knnLocal <- QueryHnswIndex(
    index = indexMin,
    queryMatrix = minMat,
    neighbors = neighborsEffective,
    ef = ef,
    nThreads = nThreads
  )

  pairs <- ExpandAnchorNeighborPairs(synCounts, knnLocal, nMinority)

  if (length(pairs$anchor) == 0L) {
    return(dataFrame[0L, , drop = FALSE])
  }

  synRaw <- InterpolateSyntheticPoints(
    minMat = minMat,
    anchorLocal = pairs$anchor,
    neighborLocal = pairs$neighbor,
    means = means,
    sds = sds,
    binaryNames = binaryNames,
    integerNames = integerNames,
    nonNegativeIntegerNames = nonNegativeIntegerNames,
    predictorNames = predictorNames,
    nThreads = nThreads
  )

  BuildSyntheticDataFrame(
    dataFrame = dataFrame,
    synRaw = synRaw,
    minorityIdx = minorityIdx,
    anchorLocal = pairs$anchor,
    predictorNames = predictorNames,
    outcomeName = outcomeName,
    minorityLevel = minorityLevel
  )
}

#' Normaliza a saida de distancias do HNSW
#'
#' @param searchResult Lista retornada por `RcppHNSW::hnsw_search()`
#' @param expectedRows Numero esperado de linhas
#' @param nThreads
#'
#' @return Vetor numerico com uma distancia media por linha consultada
#'
#' @keywords internal
ExtractDistanceVector <- function(searchResult, expectedRows, nThreads) {
  if (is.null(searchResult$dist)) {
    rlang::abort("RcppHNSW::hnsw_search() nao retornou distancias.")
  }

  if (is.matrix(searchResult$dist)) {
    avgDist <- NearMissAvgDistCpp(
      distMat = searchResult$dist,
      numThreads = as.integer(max(1L, nThreads))
    )
  } else {
    avgDist <- as.numeric(searchResult$dist)
  }

  if (length(avgDist) != expectedRows) {
    rlang::abort("As distancias retornadas pelo HNSW nao batem com a quantidade de consultas.")
  }

  if (any(!is.finite(avgDist))) {
    rlang::abort("As distancias retornadas pelo HNSW precisam ser finitas.")
  }

  avgDist
}

#' Seleciona majoritarios com NearMiss-1
#'
#' @param dataFrame Dados ja augmentados com sinteticos
#' @param xNorm Matriz normalizada correspondente a `dataFrame`
#' @param outcomeName Nome da resposta
#' @param minorityLevel Nivel minoritario
#' @param majorityLevel Nivel majoritario
#' @param neighborsNearMiss Numero de vizinhos do NearMiss-1
#' @param ef Parametro `ef`
#' @param m Parametro `M`
#' @param nThreads Numero de threads
#'
#' @return Um `data.table` balanceado
#'
#' @keywords internal
SelectNearMissRows <- function(dataFrame,
                               xNorm,
                               outcomeName,
                               minorityLevel,
                               majorityLevel,
                               neighborsNearMiss,
                               ef,
                               m,
                               nThreads) {
  yVector <- dataFrame[[outcomeName]]
  minorityIdx <- which(yVector == minorityLevel)
  majorityIdx <- which(yVector == majorityLevel)
  nMinority <- length(minorityIdx)
  nMajority <- length(majorityIdx)

  if (nMinority == 0L || nMajority == 0L) {
    return(dataFrame)
  }

  keepCount <- min(nMinority, nMajority)

  if (keepCount == nMajority) {
    return(dataFrame)
  }

  indexMin <- BuildHnswIndex(xNorm[minorityIdx, , drop = FALSE], ef, m, nThreads)

  searchResult <- RcppHNSW::hnsw_search(
    X = xNorm[majorityIdx, , drop = FALSE],
    ann = indexMin,
    k = min(neighborsNearMiss, nMinority),
    ef = ef,
    n_threads = as.integer(max(1L, nThreads)),
    grain_size = max(1000L, as.integer(nrow(xNorm[majorityIdx, , drop = FALSE]) / (nThreads * 4L))),
    byrow = TRUE
  )

  avgDist <- ExtractDistanceVector(
    searchResult = searchResult,
    expectedRows = length(majorityIdx),
    nThreads = nThreads
  )

  selectedMajorityIdx <- majorityIdx[order(avgDist, decreasing = FALSE)[seq_len(keepCount)]]

  data.table::rbindlist(
    list(
      dataFrame[minorityIdx, ],
      dataFrame[selectedMajorityIdx, ]
    ),
    use.names = TRUE
  )
}

#' Valida um numero escalar com regras configuraveis
#'
#' Funcao utilitaria para validar argumentos numericos escalares
#' Garante que o valor seja numerico, finito e dentro de um intervalo
#' especificado. Opcionalmente, pode exigir que o valor seja inteiro
#' (integerish) e/ou permitir valores nulos
#'
#' @param x Valor a ser validado
#' @param argName Nome do argumento, usado nas mensagens de erro
#' @param minValue Limite inferior permitido para o valor. Default e -Inf
#' @param maxValue Limite superior permitido para o valor. Default e Inf
#' @param integerish Logico indicando se o valor deve ser inteiro
#' @param allowNull Logico indicando se NULL e permitido
#'
#' @return Invisivelmente TRUE se a validacao for bem sucedida
#'
#' @keywords internal
ValidateSingleNumber <- function(
    x,
    argName,
    minValue = -Inf,
    maxValue = Inf,
    integerish = FALSE,
    allowNull = FALSE
) {
  if (is.null(x)) {
    if (allowNull) {
      return(invisible(TRUE))
    }

    rlang::abort(stringr::str_c("`", argName, "` nao pode ser NULL."))
  }

  if (!is.numeric(x) || length(x) != 1L || is.na(x) || !is.finite(x)) {
    rlang::abort(stringr::str_c("`", argName, "` precisa ser numerico, finito e escalar."))
  }

  if (integerish && abs(x - round(x)) > .Machine$double.eps^0.5) {
    rlang::abort(stringr::str_c("`", argName, "` precisa ser um inteiro."))
  }

  if (x < minValue || x > maxValue) {
    rlang::abort(
      stringr::str_c(
        "`", argName, "` precisa estar no intervalo [",
        minValue, ", ", maxValue, "]."
      )
    )
  }

  invisible(TRUE)
}

#' Restaura os tipos originais das colunas apos transformacoes numericas
#'
#' Funcao utilitaria para restaurar os tipos originais das colunas de um
#' data.frame apos operacoes que convertem tudo para numerico, como
#' normalizacao, interpolacao (ADASYN) e concatenacao de dados sinteticos
#'
#' A funcao utiliza um data.frame de referencia (`templateDataFrame`) para
#' identificar o tipo esperado de cada coluna e aplica a conversao adequada
#' no `dataFrame` de entrada
#'
#' Essa etapa e essencial porque operacoes numericas e combinacoes via
#' `rbindlist()` promovem automaticamente tipos para `double`, fazendo com
#' que variaveis originalmente inteiras, fatoriais ou logicas percam sua
#' classe
#'
#' Conversoes aplicadas:
#' - `factor`: reconstrucao com os niveis originais
#' - `integer`: arredondamento seguido de coerção para inteiro
#' - `numeric`: mantido como numerico
#' - `logical`: coerção para logico
#' - `Date`: reconstrucao a partir de origem unix
#' - `POSIXct`: reconstrucao preservando timezone
#' - `character`: coerção para string
#'
#' @param dataFrame Data.frame contendo os dados transformados que precisam
#'   ter os tipos restaurados
#' @param templateDataFrame Data.frame de referencia contendo os tipos
#'   originais esperados para cada coluna
#'
#' @return Um data.frame com os mesmos dados de `dataFrame`, mas com os tipos
#'   restaurados conforme `templateDataFrame`
#'
#' @details
#' A funcao atua apenas nas colunas comuns entre `dataFrame` e
#' `templateDataFrame`. Colunas extras sao ignoradas
#'
#' Para colunas inteiras, e aplicado `round()` antes da conversao para evitar
#' perda de informacao oriunda de interpolacoes numericas
#'
#' Para fatores, os niveis originais sao preservados, evitando inconsistencias
#' em pipelines de modelagem
#'
#' @keywords internal
RestoreOriginalColumnTypes <- function(dataFrame, templateDataFrame) {

  # Identifica colunas presentes em ambos os data.frames
  commonNames <- intersect(names(templateDataFrame), names(dataFrame))

  # Itera sobre cada coluna compartilhada
  for (columnName in commonNames) {

    # Coluna original (referencia de tipo)
    templateColumn <- templateDataFrame[[columnName]]

    # Coluna atual (pos transformacoes numericas)
    currentColumn <- dataFrame[[columnName]]

    # Reconstrucao de fator com niveis originais
    if (is.factor(templateColumn)) {
      dataFrame[[columnName]] <- factor(
        as.character(currentColumn),
        levels = levels(templateColumn)
      )

      # Inteiros sao restaurados via arredondamento
    } else if (is.integer(templateColumn)) {
      dataFrame[[columnName]] <- as.integer(round(currentColumn))

      # Numericos permanecem numericos
    } else if (is.numeric(templateColumn)) {
      dataFrame[[columnName]] <- as.numeric(currentColumn)

      # Logicos sao restaurados diretamente
    } else if (is.logical(templateColumn)) {
      dataFrame[[columnName]] <- as.logical(currentColumn)

      # Datas sao reconstruidas a partir da origem unix
    } else if (inherits(templateColumn, "Date")) {
      dataFrame[[columnName]] <- as.Date(currentColumn, origin = "1970-01-01")

      # POSIXct preserva timezone original
    } else if (inherits(templateColumn, "POSIXct")) {
      dataFrame[[columnName]] <- as.POSIXct(
        currentColumn,
        origin = "1970-01-01",
        tz = attr(templateColumn, "tzone", exact = TRUE)
      )

      # Strings sao restauradas como character
    } else if (is.character(templateColumn)) {
      dataFrame[[columnName]] <- as.character(currentColumn)
    }
  }

  return(dataFrame)
}
