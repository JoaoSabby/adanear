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
#' @param nThreads Inteiro positivo ou `NULL`. Numero de threads usado nas
#'   rotinas de HNSW e RcppParallel. Quando `NULL` (padrao), usa
#'   `DefaultThreadCount()` que respeita limites de CPU do ambiente e reserva
#'   margem para evitar nested parallelism agressivo. Para garantir seguranca
#'   em pipelines paralelos (ex.: `tune::tune_grid()`), defina explicitamente
#'   `nThreads = 1L`
#' @param majorityFraction Valor numerico em `(0, 1]`. Fracao da classe
#'   majoritaria usada para estimar a dificuldade do ADASYN
#' @param seed Inteiro nao negativo ou `NULL`. Semente local usada nas partes
#'   aleatorias do step. Quando `NULL` (padrao), o step usa o estado global de
#'   RNG do R (nao fixa uma semente interna). Para garantir reprodutibilidade
#'   estrita entre runs, defina explicitamente, ex.: `seed = 42L`
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
#' @export
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
                         nThreads = NULL,
                         majorityFraction = 1.0,
                         seed = NULL,
                         means = NULL,
                         sds = NULL,
                         binaryNames = NULL,
                         integerNames = NULL,
                         nonNegativeIntegerNames = NULL,
                         minorityLevel = NULL,
                         majorityLevel = NULL,
                         skip = TRUE,
                         id = recipes::rand_id("adanear")) {

  nThreads <- if (is.null(nThreads)) DefaultThreadCount() else nThreads

  # FIX PADRAO-02: em vez de passar NULL para required_pkgs.step_adanear(),
  # constroi um objeto dummy que herda a classe correta. Isso garante que o
  # despacho S3 seja semanticamente valido e resistente a atualizacoes futuras
  # da API do recipes que possam checar a classe do argumento de entrada.
  dummy_step <- structure(list(), class = c("step_adanear", "step"))
  recipes::recipes_pkg_check(required_pkgs.step_adanear(dummy_step))

  ValidateSingleNumber(increaseRatio, "increaseRatio", minValue = 0)
  ValidateSingleNumber(neighborsAdasyn, "neighborsAdasyn", minValue = 1, integerish = TRUE)
  ValidateSingleNumber(neighborsNearMiss, "neighborsNearMiss", minValue = 1, integerish = TRUE)
  ValidateSingleNumber(ef, "ef", minValue = 1, integerish = TRUE)
  ValidateSingleNumber(m, "m", minValue = 1, integerish = TRUE)
  ValidateSingleNumber(nThreads, "nThreads", minValue = 1, integerish = TRUE)
  ValidateSingleNumber(
    majorityFraction,
    "majorityFraction",
    minValue = .Machine$double.eps,
    maxValue = 1
  )

  ValidateSingleNumber(seed, "seed", minValue = 0, integerish = TRUE, allowNull = TRUE)
  seedStored <- if (is.null(seed)) NA_integer_ else as.integer(seed)

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
      seed = seedStored,
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
#' @exportS3Method recipes::prep
prep.step_adanear <- function(x, training, info = NULL, ...) {
  selectedColumn <- ResolveSamplingColumn(x$terms, training, info)
  ValidateOutcomeRole(selectedColumn, info)

  predictorNames <- ResolvePredictorNames(selectedColumn, info)

  ValidateOutcome(training, selectedColumn)
  ValidateNumericPredictors(training, predictorNames)
  ValidateIntegerPrecisionRisk(training, predictorNames)

  xMatrix <- as.matrix(training[, predictorNames, drop = FALSE])
  normStats <- BuildNormalizationStats(xMatrix)
  typeNames <- DetectPredictorTypes(training, predictorNames)

  # FIX VALID-01: usar droplevels() para que niveis com contagem zero nao
  # influenciem o min() em ValidateAdasynFeasibility(), evitando falsos positivos
  # quando o fator contem niveis vazios (e.g. apos subsetting do fold de treino).
  classTable <- table(droplevels(training[[selectedColumn]]))
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
#' @exportS3Method recipes::bake
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

  if (is.na(object$seed)) {
    ExecuteBake()
  } else {
    withr::with_seed(object$seed, ExecuteBake())
  }
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
#' @exportS3Method base::print
print.step_adanear <- function(x, width = max(20, getOption("width") - 30), ...) {
  termLabel <- if (recipes::is_trained(x)) x$column else recipes::sel2char(x$terms)
  seedLabel <- if (length(x$seed) == 1L && is.na(x$seed)) "global_rng" else as.character(x$seed)

  cat("ADASYN + NearMiss sampling on ")
  recipes::printer(termLabel, x$terms, x$trained, width = width)
  cat(
    sprintf(
      "  increaseRatio: %.3f | neighborsAdasyn: %d | neighborsNearMiss: %d | ef: %d | M: %d | threads: %d | majFrac: %.2f | seed: %s\n",
      x$increaseRatio,
      x$neighborsAdasyn,
      x$neighborsNearMiss,
      x$ef,
      x$m,
      x$nThreads,
      x$majorityFraction,
      seedLabel
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
#' @exportS3Method recipes::tidy
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
    seed = if (length(x$seed) == 1L && is.na(x$seed)) NA_integer_ else x$seed,
    id = x$id
  )
}

#' Parametros ajustaveis de `step_adanear()`
#'
#' Expõe os hiperparâmetros que podem ser otimizados via `tune`/`dials`.
#'
#' @param x Um objeto `step_adanear`
#' @param ... Nao usado
#'
#' @return Um tibble no formato esperado por `dials::tunable()`
#'
#' @rdname step_adanear
# Registro S3 de `tunable` e feito condicionalmente em `.onLoad()`
# para manter compatibilidade com versoes do `dials` que nao exportam esse
# generico.
tunable.step_adanear <- function(x, ...) {
  tibble::tibble(
    name = c(
      "increaseRatio",
      "neighborsAdasyn",
      "neighborsNearMiss",
      "majorityFraction"
    ),
    call_info = list(
      list(pkg = "dials", fun = "over_ratio", range = c(NA, NA), trans = NULL),
      list(pkg = "dials", fun = "neighbors", range = c(NA, NA), trans = NULL),
      list(pkg = "dials", fun = "neighbors", range = c(NA, NA), trans = NULL),
      list(pkg = "dials", fun = "frac_pt", range = c(NA, NA), trans = NULL)
    ),
    source = "recipe",
    component = "step_adanear",
    component_id = x$id
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
#' @exportS3Method recipes::required_pkgs
required_pkgs.step_adanear <- function(x, ...) {
  c(
    "recipes",
    "rlang",
    "tibble",
    "data.table",
    "RcppHNSW",
    "parallelly",
    "parallel",
    "withr",
    "stringr"
  )
}

# FIX PADRAO-01: todas as funcoes helpers passam a usar @noRd em vez de
# @keywords internal, evitando paginas Rd orphas e possiveis NOTEs no R CMD check.

#' @noRd
DefaultThreadCount <- function() {
  available <- tryCatch(
    parallelly::availableCores(omit = 1L),
    error = function(...) parallel::detectCores(logical = TRUE)
  )

  available <- as.integer(available[[1]])

  if (length(available) != 1L || is.na(available) || available < 1L) {
    return(1L)
  }

  as.integer(max(1L, available))
}

#' @noRd
minMinoritySize <- 2L

#' @noRd
integerTolerance <- 1e-8

#' @noRd
ResolveSamplingColumn <- function(terms, training, info) {
  selected <- recipes::recipes_eval_select(terms, training, info)
  selectedNames <- names(selected)

  # FIX PADRAO-03: removido o fallback posicional fragil que usava
  # unname(selected) como indice de colnames(). O contrato de
  # recipes_eval_select() garante nomes quando ha selecao valida.
  # Se nao ha nomes, a selecao falhou e devemos abortar imediatamente.
  if (length(selectedNames) != 1L) {
    rlang::abort("step_adanear() requer que `...` selecione exatamente uma coluna fator binaria.")
  }

  selectedNames[[1]]
}

#' @noRd
ValidateOutcomeRole <- function(column, info) {
  outcomeNames <- info$variable[info$role == "outcome"]

  if (!(column %in% outcomeNames)) {
    rlang::abort("step_adanear() requer que `...` selecione a variavel resposta do recipe.")
  }

  invisible(TRUE)
}

#' @noRd
ResolvePredictorNames <- function(column, info) {
  predictorNames <- info$variable[info$role == "predictor"]
  predictorNames <- setdiff(predictorNames, column)

  if (length(predictorNames) == 0L) {
    rlang::abort("step_adanear() requer pelo menos um preditor com papel de predictor no recipe.")
  }

  predictorNames
}

#' @noRd
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

#' @noRd
ValidateNumericPredictors <- function(training, predictorNames) {
  if (length(predictorNames) == 0L) {
    rlang::abort("step_adanear() requer pelo menos um preditor.")
  }

  predictorFrame <- training[, predictorNames, drop = FALSE]

  isNumeric <- vapply(predictorFrame, is.numeric, logical(1))
  if (!all(isNumeric)) {
    badNames <- names(isNumeric)[!isNumeric]
    rlang::abort(
      stringr::str_c(
        "Todos os preditores precisam ser numericos. Problemas em: ",
        stringr::str_c(badNames, collapse = ", "),
        ". Aplique imputacao, encoding e demais passos antes de step_adanear()."
      )
    )
  }

  hasMissing <- vapply(predictorFrame, anyNA, logical(1))
  if (any(hasMissing)) {
    badNames <- names(hasMissing)[hasMissing]
    rlang::abort(
      stringr::str_c(
        "Todos os preditores precisam estar sem NA antes de step_adanear(). Problemas em: ",
        stringr::str_c(badNames, collapse = ", ")
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
      stringr::str_c(
        "Todos os preditores precisam conter apenas valores finitos. Problemas em: ",
        stringr::str_c(badNames, collapse = ", ")
      )
    )
  }

  invisible(TRUE)
}

#' @noRd
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

  if (length(object$seed) != 1L) {
    rlang::abort("`seed` precisa ser escalar ou NA_integer_.")
  }

  if (!is.na(object$seed)) {
    ValidateSingleNumber(object$seed, "seed", minValue = 0, integerish = TRUE)
  }

  ValidateSingleNumber(object$nThreads, "nThreads", minValue = 1, integerish = TRUE)

  invisible(TRUE)
}

#' @noRd
ValidateAdasynFeasibility <- function(classTable, increaseRatio) {
  if (increaseRatio <= 0) {
    return(invisible(TRUE))
  }

  minorityCount <- min(as.integer(classTable))

  if (minorityCount < minMinoritySize) {
    rlang::abort(
      stringr::str_c(
        "Com `increaseRatio > 0`, a classe minoritaria precisa ter pelo menos ",
        minMinoritySize,
        " observacoes."
      )
    )
  }

  invisible(TRUE)
}

#' @noRd
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

#' @noRd
# FIX BUG-02: aplicar droplevels() internamente para que niveis de fator com
# contagem zero nao influenciem which.min() / which.max(), evitando que
# minority e majority sejam atribuidos incorretamente apos subsetting de folds
# de validacao cruzada onde um nivel pode existir no fator mas ter zero linhas.
DetectClassLevels <- function(yVector) {
  classTable <- table(droplevels(yVector))

  if (length(unique(as.integer(classTable))) == 1L) {
    rlang::abort("As classes estao empatadas. Nao ha minoria e maioria bem definidas.")
  }

  list(
    minority = names(classTable)[which.min(classTable)],
    majority = names(classTable)[which.max(classTable)]
  )
}

#' @noRd
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

#' @noRd
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

#' @noRd
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

#' @noRd
ValidateIntegerPrecisionRisk <- function(training, predictorNames) {
  if (length(predictorNames) == 0L) {
    return(invisible(TRUE))
  }

  safeIntegerLimit <- 2^53

  isWhole <- vapply(
    training[, predictorNames, drop = FALSE],
    IsWholeNumber,
    logical(1)
  )

  candidateNames <- predictorNames[isWhole]
  if (length(candidateNames) == 0L) {
    return(invisible(TRUE))
  }

  riskyNames <- candidateNames[
    vapply(
      training[, candidateNames, drop = FALSE],
      function(column) any(abs(column[is.finite(column)]) > safeIntegerLimit),
      logical(1)
    )
  ]

  if (length(riskyNames) > 0L) {
    rlang::abort(
      stringr::str_c(
        "Foram detectados inteiros acima de 2^53 em preditores: ",
        stringr::str_c(riskyNames, collapse = ", "),
        ". Para evitar perda de precisao em interpolacao, remova IDs/chaves do step_adanear() ou transforme-os antes."
      )
    )
  }

  invisible(TRUE)
}

#' @noRd
ComputeSafeGrainSize <- function(nRows, nThreads) {
  ValidateSingleNumber(nRows, "nRows", minValue = 1L, integerish = TRUE)
  ValidateSingleNumber(nThreads, "nThreads", minValue = 1L, integerish = TRUE)

  # Usa aritmetica em double para evitar overflow de inteiro no denominador.
  denominator <- max(1, as.double(nThreads) * 4.0)
  grain <- as.integer(floor(as.double(nRows) / denominator))
  max(1000L, grain)
}

#' @noRd
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

  safeGrain <- ComputeSafeGrainSize(nrow(xMatrix), nThreads)

  RcppHNSW::hnsw_build(
    X = xMatrix,
    distance = "euclidean",
    M = as.integer(m),
    ef = as.integer(ef),
    n_threads = as.integer(max(1L, nThreads)),
    grain_size = safeGrain,
    byrow = TRUE,
    verbose = FALSE
  )
}

#' @noRd
QueryHnswIndex <- function(index, queryMatrix, neighbors, ef, nThreads) {
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

  ValidateSingleNumber(neighbors, "neighbors", minValue = 1L, integerish = TRUE)
  ValidateSingleNumber(ef, "ef", minValue = 1L, integerish = TRUE)
  ValidateSingleNumber(nThreads, "nThreads", minValue = 1L, integerish = TRUE)

  safeGrain <- ComputeSafeGrainSize(nrow(queryMatrix), nThreads)

  searchResult <- RcppHNSW::hnsw_search(
    X = queryMatrix,
    ann = index,
    k = as.integer(neighbors),
    ef = as.integer(ef),
    n_threads = as.integer(max(1L, nThreads)),
    grain_size = safeGrain,
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

#' @noRd
# FIX BUG-01: wrapper que retorna tanto indices quanto distancias do HNSW,
# centralizando validacoes (is.matrix, anyNA, is.finite, dim fix) e o calculo
# seguro de grain_size. SelectNearMissRows passa a usar este wrapper em vez de
# chamar RcppHNSW::hnsw_search() diretamente, o que eliminava todas essas
# salvaguardas e duplicava a logica de grain_size.
QueryHnswIndexWithDist <- function(index, queryMatrix, neighbors, ef, nThreads) {
  if (!is.matrix(queryMatrix)) {
    rlang::abort("QueryHnswIndexWithDist() espera uma matriz de consulta.")
  }

  if (!is.numeric(queryMatrix)) {
    rlang::abort("QueryHnswIndexWithDist() espera uma matriz numerica.")
  }

  if (nrow(queryMatrix) == 0L) {
    rlang::abort("QueryHnswIndexWithDist() recebeu zero linhas de consulta.")
  }

  if (ncol(queryMatrix) == 0L) {
    rlang::abort("QueryHnswIndexWithDist() recebeu zero colunas de consulta.")
  }

  if (anyNA(queryMatrix) || any(!is.finite(queryMatrix))) {
    rlang::abort("QueryHnswIndexWithDist() recebeu valores NA, NaN ou Inf.")
  }

  ValidateSingleNumber(neighbors, "neighbors", minValue = 1L, integerish = TRUE)
  ValidateSingleNumber(ef, "ef", minValue = 1L, integerish = TRUE)
  ValidateSingleNumber(nThreads, "nThreads", minValue = 1L, integerish = TRUE)

  safeGrain <- ComputeSafeGrainSize(nrow(queryMatrix), nThreads)

  searchResult <- RcppHNSW::hnsw_search(
    X = queryMatrix,
    ann = index,
    k = as.integer(neighbors),
    ef = as.integer(ef),
    n_threads = as.integer(max(1L, nThreads)),
    grain_size = safeGrain,
    byrow = TRUE
  )

  idx <- searchResult$idx
  dst <- searchResult$dist

  if (is.null(idx)) {
    rlang::abort("RcppHNSW::hnsw_search() nao retornou indices.")
  }

  if (is.null(dst)) {
    rlang::abort("RcppHNSW::hnsw_search() nao retornou distancias.")
  }

  if (is.null(dim(idx))) {
    idx <- matrix(as.integer(idx), nrow = nrow(queryMatrix), byrow = TRUE)
  }

  if (!identical(nrow(idx), nrow(queryMatrix))) {
    rlang::abort("A matriz de indices retornada pelo HNSW veio com numero inesperado de linhas.")
  }

  storage.mode(idx) <- "integer"

  list(idx = idx, dist = dst)
}

#' @noRd
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

#' @noRd
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
      stringr::str_c(
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

#' @noRd
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

#' @noRd
ResolveColumnIndices <- function(targetNames, predictorNames) {
  if (length(targetNames) == 0L) {
    return(integer(0))
  }

  idx <- match(targetNames, predictorNames)

  if (anyNA(idx)) {
    missingNames <- targetNames[is.na(idx)]
    rlang::abort(
      stringr::str_c(
        "Falha ao mapear colunas para restauracao de tipos. Problemas em: ",
        stringr::str_c(missingNames, collapse = ", ")
      )
    )
  }

  as.integer(idx - 1L)
}

#' @noRd
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
) {

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

#' @noRd
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

#' @noRd
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
    rlang::abort(stringr::str_c("ADASYN requer pelo menos ", minMinoritySize, " observacoes minoritarias."))
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

#' @noRd
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

#' @noRd
# FIX BUG-01: substituida chamada direta a RcppHNSW::hnsw_search() por
# QueryHnswIndexWithDist(), que centraliza validacoes de entrada (is.matrix,
# anyNA, is.finite, dim fix) e o calculo seguro de grain_size — mantendo
# consistencia com todos os outros pontos de acesso ao HNSW no codigo.
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

  searchResult <- QueryHnswIndexWithDist(
    index = indexMin,
    queryMatrix = xNorm[majorityIdx, , drop = FALSE],
    neighbors = min(neighborsNearMiss, nMinority),
    ef = ef,
    nThreads = nThreads
  )

  avgDist <- ExtractDistanceVector(
    searchResult = searchResult,
    expectedRows = nMajority,
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

#' @noRd
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

#' @noRd
RestoreOriginalColumnTypes <- function(dataFrame, templateDataFrame) {

  commonNames <- intersect(names(templateDataFrame), names(dataFrame))

  for (columnName in commonNames) {
    templateColumn <- templateDataFrame[[columnName]]
    currentColumn  <- dataFrame[[columnName]]

    if (is.factor(templateColumn)) {
      dataFrame[[columnName]] <- factor(
        as.character(currentColumn),
        levels = levels(templateColumn)
      )
    } else if (is.integer(templateColumn)) {
      dataFrame[[columnName]] <- as.integer(round(currentColumn))
    } else if (is.numeric(templateColumn)) {
      dataFrame[[columnName]] <- as.numeric(currentColumn)
    } else if (is.logical(templateColumn)) {
      dataFrame[[columnName]] <- as.logical(currentColumn)
    } else if (inherits(templateColumn, "Date")) {
      dataFrame[[columnName]] <- as.Date(currentColumn, origin = "1970-01-01")
    } else if (inherits(templateColumn, "POSIXct")) {
      dataFrame[[columnName]] <- as.POSIXct(
        currentColumn,
        origin = "1970-01-01",
        tz = attr(templateColumn, "tzone", exact = TRUE)
      )
    } else if (is.character(templateColumn)) {
      dataFrame[[columnName]] <- as.character(currentColumn)
    }
  }

  dataFrame
}
