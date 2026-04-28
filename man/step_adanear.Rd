\name{step_adanear}
\alias{step_adanear}
\alias{prep.step_adanear}
\alias{bake.step_adanear}
\alias{print.step_adanear}
\alias{tidy.step_adanear}
\alias{tunable.step_adanear}
\alias{required_pkgs.step_adanear}
\title{ADASYN + NearMiss em um step customizado do recipes}
\usage{
step_adanear(
  recipe,
  ...,
  role = NA,
  trained = FALSE,
  column = NULL,
  predictors = NULL,
  increaseRatio = 0.2,
  neighborsAdasyn = 5L,
  neighborsNearMiss = 5L,
  under_ratio = 1,
  ef = 200L,
  m = 16L,
  nThreads = NULL,
  majorityFraction = 1,
  seed = NULL,
  means = NULL,
  sds = NULL,
  binaryNames = NULL,
  integerNames = NULL,
  nonNegativeIntegerNames = NULL,
  minorityLevel = NULL,
  majorityLevel = NULL,
  skip = TRUE,
  id = recipes::rand_id("adanear")
)
}
\arguments{
\item{recipe}{Um objeto \code{recipe}.}
\item{...}{Seletores para a variável resposta usada no balanceamento.}
\item{role}{Não usado por este step.}
\item{trained}{Indica se o step já foi treinado.}
\item{column}{Nome da coluna resposta resolvida em \code{prep()}.}
\item{predictors}{Nomes dos preditores usados para distância.}
\item{increaseRatio}{Razão de aumento da classe minoritária.}
\item{neighborsAdasyn}{Número de vizinhos usados no ADASYN.}
\item{neighborsNearMiss}{Número de vizinhos minoritários no NearMiss-1.}
\item{under_ratio}{Valor numérico maior ou igual a 1 que define quantos
registros da maioria serão mantidos em relação ao tamanho da minoria após a
adição de sintéticos no ADASYN. Com \code{under_ratio = 1}, as classes se
igualam; com \code{under_ratio = 1.3}, a maioria final terá 1.3 vezes a
quantidade da minoria.}
\item{ef}{Parâmetro \code{ef} do HNSW.}
\item{m}{Parâmetro \code{M} do HNSW.}
\item{nThreads}{Número de threads; \code{NULL} usa detecção automática com limites seguros do ambiente.}
\item{majorityFraction}{Fração da maioria usada no ADASYN.}
\item{seed}{Semente local para as etapas aleatórias; \code{NULL} usa o RNG global.}
\item{means}{Médias por coluna para normalização.}
\item{sds}{Desvios padrão por coluna para normalização.}
\item{binaryNames}{Nomes de preditores binários.}
\item{integerNames}{Nomes de preditores inteiros não-binários.}
\item{nonNegativeIntegerNames}{Subset de \code{integerNames} não-negativos.}
\item{minorityLevel}{Nível minoritário aprendido no \code{prep()}.}
\item{majorityLevel}{Nível majoritário aprendido no \code{prep()}.}
\item{skip}{Se \code{TRUE}, aplica só no treino.}
\item{id}{Identificador único do step.}
}
\value{Um objeto \code{recipe} com o step \code{step_adanear} adicionado.}
\description{
\code{step_adanear()} cria um step supervisionado de reamostragem para
classificação binária, combinando geração sintética (ADASYN) e seleção
majoritária (NearMiss-1).
}
\details{
A variável selecionada em \code{...} deve ser um fator binário sem \code{NA}, e
os preditores precisam ser numéricos, finitos e sem \code{NA}.
}
\examples{
\dontrun{
recipes::recipe(target ~ ., data = dadosTreino) |>
  recipes::step_dummy(recipes::all_nominal_predictors(), one_hot = TRUE) |>
  step_adanear(target, increaseRatio = 0.2, seed = 42L)
}
}
