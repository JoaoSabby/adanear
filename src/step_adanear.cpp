// Rotinas C++ usadas pelo step_adanear
// Aqui ficam os kernels pesados de normalizacao, ADASYN, NearMiss e
// restauracao de tipos. As funcoes exportadas validam contrato antes de
// entrar no trecho paralelo para evitar erro silencioso vindo do lado R

// [[Rcpp::depends(RcppParallel)]]
// [[Rcpp::plugins(cpp17)]]

#include <Rcpp.h>
#include <RcppParallel.h>
#include <algorithm>
#include <cmath>
#include <string>
#include <vector>

using namespace Rcpp;
using namespace RcppParallel;

// Helpers de validacao usados nas funcoes exportadas
// Como essas rotinas tambem podem ser chamadas direto do R, vale falhar cedo
// com erro claro em vez de depender so do chamador em R
inline void AbortIf(bool condition, const std::string& message){
  if(condition){
    stop(message);
  }
}


// Valida vetores estatisticos
inline void ValidateStatVectors(
    const NumericMatrix& x,
    const NumericVector& means,
    const NumericVector& sds,
    const std::string& callerName
){

  AbortIf(
    means.size() != x.ncol(),
    callerName + ": `means` precisa ter o mesmo tamanho de `ncol(x)`."
  );

  AbortIf(
    sds.size() != x.ncol(),
    callerName + ": `sds` precisa ter o mesmo tamanho de `ncol(x)`."
  );

  for(int colIdx = 0; colIdx < x.ncol(); ++colIdx){
    const double sdValue = sds[colIdx];
    AbortIf(
      NumericVector::is_na(sdValue) || !R_finite(sdValue) || sdValue == 0.0,
      callerName + ": `sds` precisa conter apenas valores finitos e diferentes de zero."
    );
  }
}

inline void ValidateFiniteMatrix(const NumericMatrix& x,
                                 const std::string& callerName){
  for(int rowIdx = 0; rowIdx < x.nrow(); ++rowIdx){
    for(int colIdx = 0; colIdx < x.ncol(); ++colIdx){
      const double value = x(rowIdx, colIdx);
      AbortIf(
        NumericVector::is_na(value) || !R_finite(value),
        callerName + ": a matriz precisa conter apenas valores finitos."
      );
    }
  }
}

inline void ValidateBinaryVector(const IntegerVector& yBinary,
                                 const std::string& callerName){
  for(int i = 0; i < yBinary.size(); ++i){
    const int value = yBinary[i];
    AbortIf(
      value == NA_INTEGER || (value != 0 && value != 1),
      callerName + ": `yBinary` precisa conter apenas 0 e 1."
    );
  }
}

inline void ValidateIndexVector(const IntegerVector& idx,
                                const int lowerBound,
                                const int upperBound,
                                const std::string& objectName,
                                const bool allowNa = false){
  for(int i = 0; i < idx.size(); ++i){
    const int value = idx[i];

    if(allowNa && value == NA_INTEGER){
      continue;
    }

    AbortIf(
      value == NA_INTEGER || value < lowerBound || value > upperBound,
      objectName + " contem indice fora do intervalo esperado."
    );
  }
}

inline void ValidateColumnIndices(const IntegerVector& colIdxs,
                                  const int nCols,
                                  const std::string& objectName){
  for(int i = 0; i < colIdxs.size(); ++i){
    const int colIdx = colIdxs[i];
    AbortIf(
      colIdx == NA_INTEGER || colIdx < 0 || colIdx >= nCols,
      objectName + " contem indice de coluna invalido."
    );
  }
}

// Worker da normalizacao z-score por blocos de linhas
struct NormalizeWorker : public Worker {
  const RMatrix<double> input;
  RMatrix<double> output;
  const RVector<double> means;
  const RVector<double> sds;

  NormalizeWorker(const NumericMatrix& input,
                  NumericMatrix& output,
                  const NumericVector& means,
                  const NumericVector& sds)
    : input(input), output(output), means(means), sds(sds){}

  void operator()(std::size_t begin, std::size_t end){
    const int nCols = static_cast<int>(input.ncol());

    for(std::size_t rowIdx = begin; rowIdx < end; ++rowIdx){
      for(int colIdx = 0; colIdx < nCols; ++colIdx){
        output(rowIdx, colIdx) = (input(rowIdx, colIdx) - means[colIdx]) / sds[colIdx];
      }
    }
  }
};

//' Normaliza uma matriz numerica em paralelo
 //'
 //' Aplica z-score coluna a coluna com medias e desvios informados pelo lado R
 //' A funcao valida dimensoes antes de entrar no loop paralelo
 //'
 //' @param x Matriz numerica de entrada
 //' @param means Vetor com a media de cada coluna
 //' @param sds Vetor com o desvio padrao de cada coluna
 //'
 //' @return Matriz com a mesma dimensao de `x`, normalizada por coluna
 //'
 //' @keywords internal
 // [[Rcpp::export]]
 NumericMatrix NormalizeMatrixCpp(
     const NumericMatrix& x,
     const NumericVector& means,
     const NumericVector& sds,
     const int numThreads
 ){

   ValidateStatVectors(x, means, sds, "NormalizeMatrixCpp");
   ValidateFiniteMatrix(x, "NormalizeMatrixCpp");

   NumericMatrix output(x.nrow(), x.ncol());

   if(x.nrow() == 0 || x.ncol() == 0){
     return output;
   }

   NormalizeWorker worker(x, output, means, sds);

   AbortIf(
     numThreads < 1, "NormalizeMatrixCpp: `numThreads` precisa ser pelo menos 1."
   );

   parallelFor(0, static_cast<std::size_t>(x.nrow()), worker, 512, numThreads);

   return output;
 }

 // Worker da volta para a escala original por blocos de linhas
 struct DenormalizeWorker : public Worker {
   const RMatrix<double> input;
   RMatrix<double> output;
   const RVector<double> means;
   const RVector<double> sds;

   DenormalizeWorker(const NumericMatrix& input,
                     NumericMatrix& output,
                     const NumericVector& means,
                     const NumericVector& sds)
     : input(input), output(output), means(means), sds(sds){}

   void operator()(std::size_t begin, std::size_t end){
     const int nCols = static_cast<int>(input.ncol());

     for(std::size_t rowIdx = begin; rowIdx < end; ++rowIdx){
       for(int colIdx = 0; colIdx < nCols; ++colIdx){
         output(rowIdx, colIdx) = input(rowIdx, colIdx) * sds[colIdx] + means[colIdx];
       }
     }
   }
 };

 //' Desnormaliza uma matriz numerica em paralelo
 //'
 //' Reverte a transformacao aplicada por `NormalizeMatrixCpp()`
 //'
 //' @param x Matriz normalizada
 //' @param means Vetor com a media de cada coluna
 //' @param sds Vetor com o desvio padrao de cada coluna
 //'
 //' @return Matriz na escala original
 //'
 //' @keywords internal
 // [[Rcpp::export]]
 NumericMatrix DenormalizeMatrixCpp(
     const NumericMatrix& x,
     const NumericVector& means,
     const NumericVector& sds,
     const int numThreads
 ){

   ValidateStatVectors(x, means, sds, "DenormalizeMatrixCpp");
   ValidateFiniteMatrix(x, "DenormalizeMatrixCpp");

   NumericMatrix output(x.nrow(), x.ncol());

   if(x.nrow() == 0 || x.ncol() == 0){
     return output;
   }

   DenormalizeWorker worker(x, output, means, sds);

   AbortIf(
     numThreads < 1, "DenormalizeMatrixCpp: `numThreads` precisa ser pelo menos 1."
   );

   parallelFor(0, static_cast<std::size_t>(x.nrow()), worker, 512, numThreads);

   return output;
 }

 // Worker que gera cada sintetico por interpolacao linear independente
 struct AdasynInterpolateWorker : public Worker {
   const RMatrix<double> minorityMatrix;
   const RVector<int> anchorIdx;
   const RVector<int> neighborIdx;
   const RVector<double> lambdas;
   RMatrix<double> output;

   AdasynInterpolateWorker(const NumericMatrix& minorityMatrix,
                           const IntegerVector& anchorIdx,
                           const IntegerVector& neighborIdx,
                           const NumericVector& lambdas,
                           NumericMatrix& output)
     : minorityMatrix(minorityMatrix),
       anchorIdx(anchorIdx),
       neighborIdx(neighborIdx),
       lambdas(lambdas),
       output(output){}

   void operator()(std::size_t begin, std::size_t end){
     const int nCols = static_cast<int>(minorityMatrix.ncol());

     for(std::size_t syntheticIdx = begin; syntheticIdx < end; ++syntheticIdx){
       const int anchor = anchorIdx[syntheticIdx];
       const int neighbor = neighborIdx[syntheticIdx];
       const double lambda = lambdas[syntheticIdx];

       for(int colIdx = 0; colIdx < nCols; ++colIdx){
         output(syntheticIdx, colIdx) =
           minorityMatrix(anchor, colIdx) +
           lambda * (minorityMatrix(neighbor, colIdx) - minorityMatrix(anchor, colIdx));
       }
     }
   }
 };

 //' Gera sinteticos por interpolacao linear em paralelo
 //'
 //' Cada linha do resultado e produzida a partir de um par ancora-vizinho e de
 //' um lambda em `[0, 1]`
 //'
 //' @param minMat Matriz normalizada contendo so a minoria
 //' @param anchorIdx Vetor 0-based com os indices das ancoras
 //' @param neighborIdx Vetor 0-based com os indices dos vizinhos
 //' @param lambdas Vetor com os fatores de interpolacao
 //'
 //' @return Matriz com os sinteticos no espaco normalizado
 //'
 //' @keywords internal
 // [[Rcpp::export]]
 NumericMatrix AdasynInterpolateCpp(
     const NumericMatrix& minMat,
     const IntegerVector& anchorIdx,
     const IntegerVector& neighborIdx,
     const NumericVector& lambdas,
     const int numThreads
 ){

   ValidateFiniteMatrix(minMat, "AdasynInterpolateCpp");

   AbortIf(
     anchorIdx.size() != neighborIdx.size() || anchorIdx.size() != lambdas.size(),
     "AdasynInterpolateCpp: `anchorIdx`, `neighborIdx` e `lambdas` precisam ter o mesmo tamanho."
   );

   NumericMatrix output(anchorIdx.size(), minMat.ncol());

   if(anchorIdx.size() == 0 || minMat.ncol() == 0){
     return output;
   }

   ValidateIndexVector(anchorIdx, 0, minMat.nrow() - 1, "AdasynInterpolateCpp(anchorIdx)");
   ValidateIndexVector(neighborIdx, 0, minMat.nrow() - 1, "AdasynInterpolateCpp(neighborIdx)");

   for(int i = 0; i < lambdas.size(); ++i){
     const double lambda = lambdas[i];
     AbortIf(
       NumericVector::is_na(lambda) || !R_finite(lambda) || lambda < 0.0 || lambda > 1.0,
       "AdasynInterpolateCpp: `lambdas` precisa conter apenas valores finitos entre 0 e 1."
     );
   }

   AdasynInterpolateWorker worker(minMat, anchorIdx, neighborIdx, lambdas, output);

   AbortIf(
     numThreads < 1, "AdasynInterpolateCpp: `numThreads` precisa ser pelo menos 1."
   );

   parallelFor(0, static_cast<std::size_t>(anchorIdx.size()), worker, 1024, numThreads);

   return output;
 }

 // Worker que mede a proporcao local de vizinhos majoritarios
 struct DifficultyWorker : public Worker {
   const RMatrix<int> knnIdx;
   const RVector<int> minorityMixedIdx;
   const RVector<int> yBinary;
   RVector<double> difficulty;

   DifficultyWorker(const IntegerMatrix& knnIdx,
                    const IntegerVector& minorityMixedIdx,
                    const IntegerVector& yBinary,
                    NumericVector& difficulty)
     : knnIdx(knnIdx),
       minorityMixedIdx(minorityMixedIdx),
       yBinary(yBinary),
       difficulty(difficulty){}

   void operator()(std::size_t begin, std::size_t end){
     const int k = static_cast<int>(knnIdx.ncol());

     for(std::size_t rowIdx = begin; rowIdx < end; ++rowIdx){
       const int selfIdx = minorityMixedIdx[rowIdx];
       int majorityCount = 0;
       int validNeighbors = 0;

       for(int neighborPos = 0; neighborPos < k; ++neighborPos){
         const int neighborIdx = knnIdx(rowIdx, neighborPos);

         if(neighborIdx != selfIdx){
           majorityCount += yBinary[neighborIdx - 1];
           ++validNeighbors;
         }
       }

       difficulty[rowIdx] =
         (validNeighbors > 0) ? static_cast<double>(majorityCount) / validNeighbors : 0.0;
     }
   }
 };

 //' Calcula a dificuldade local do ADASYN em paralelo
 //'
 //' Para cada observacao minoritaria, calcula a proporcao de vizinhos
 //' majoritarios entre os vizinhos retornados pelo HNSW
 //'
 //' @param knnIdx Matriz inteira `nMinoria x k` com indices 1-based
 //' @param minGlobalIdx Vetor 1-based com a posicao da minoria dentro do
 //'   dataset misto usado na busca
 //' @param yBinary Vetor inteiro com 0 para minoria e 1 para maioria
 //'
 //' @return Vetor numerico com uma dificuldade por linha minoritaria
 //'
 //' @keywords internal
 // [[Rcpp::export]]
 NumericVector ComputeDifficultyCpp(
     const IntegerMatrix& knnIdx,
     const IntegerVector& minGlobalIdx,
     const IntegerVector& yBinary,
     const int numThreads
 ){

   ValidateBinaryVector(yBinary, "ComputeDifficultyCpp");

   AbortIf(
     knnIdx.nrow() != minGlobalIdx.size(),
     "ComputeDifficultyCpp: `nrow(knnIdx)` precisa ser igual ao tamanho de `minGlobalIdx`."
   );

   AbortIf(
     yBinary.size() <= 0, "ComputeDifficultyCpp: `yBinary` nao pode estar vazio."
   );

   if(minGlobalIdx.size() == 0){
     return NumericVector(0);
   }

   ValidateIndexVector(
     minGlobalIdx,
     1,
     yBinary.size(),
     "ComputeDifficultyCpp(minGlobalIdx)"
   );

   for(int rowIdx = 0; rowIdx < knnIdx.nrow(); ++rowIdx){
     for(int colIdx = 0; colIdx < knnIdx.ncol(); ++colIdx){
       const int neighborIdx = knnIdx(rowIdx, colIdx);
       AbortIf(
         neighborIdx == NA_INTEGER || neighborIdx < 1 || neighborIdx > yBinary.size(),
         "ComputeDifficultyCpp: `knnIdx` contem indice invalido."
       );
     }
   }

   NumericVector difficulty(minGlobalIdx.size());
   DifficultyWorker worker(knnIdx, minGlobalIdx, yBinary, difficulty);

   AbortIf(
     numThreads < 1, "ComputeDifficultyCpp: `numThreads` precisa ser pelo menos 1."
   );

   parallelFor(0, static_cast<std::size_t>(minGlobalIdx.size()), worker, 256, numThreads);

   return difficulty;
 }

 //' Amostra um vizinho valido para cada ancora
 //'
 //' A amostragem e propositalmente single-thread porque usa o RNG do R. A funcao
 //' ignora o auto-vizinho e devolve `NA_INTEGER` quando nao encontra candidato
 //'
 //' @param knnLocal Matriz `nMinoria x k` com indices locais 1-based
 //' @param anchorLocalIdx Vetor 1-based indicando a linha de `knnLocal` de cada
 //'   ancora
 //'
 //' @return Vetor 1-based com o vizinho amostrado por ancora
 //'
 //' @keywords internal
 // [[Rcpp::export]]
 IntegerVector SampleNeighborsCpp(const IntegerMatrix& knnLocal,
                                  const IntegerVector& anchorLocalIdx){
   const int nSynthetic = anchorLocalIdx.size();
   const int k = knnLocal.ncol();
   IntegerVector result(nSynthetic, NA_INTEGER);

   if(nSynthetic == 0){
     return result;
   }

   AbortIf(
     knnLocal.nrow() <= 0 || knnLocal.ncol() <= 0, "SampleNeighborsCpp: `knnLocal` precisa ter pelo menos uma linha e uma coluna."
   );

   ValidateIndexVector(
     anchorLocalIdx,
     1,
     knnLocal.nrow(),
     "SampleNeighborsCpp(anchorLocalIdx)"
   );

   RNGScope rngScope;

   for(int syntheticIdx = 0; syntheticIdx < nSynthetic; ++syntheticIdx){
     if(syntheticIdx % 10000 == 0){
       checkUserInterrupt();
     }

     const int anchorRow = anchorLocalIdx[syntheticIdx] - 1;
     std::vector<int> candidates;
     candidates.reserve(k);

     for(int neighborPos = 0; neighborPos < k; ++neighborPos){
       const int neighborIdx = knnLocal(anchorRow, neighborPos);

       AbortIf(
         neighborIdx == NA_INTEGER || neighborIdx < 1 || neighborIdx > knnLocal.nrow(),
         "SampleNeighborsCpp: `knnLocal` contem indice invalido."
       );

       if(neighborIdx != (anchorRow + 1)){
         candidates.push_back(neighborIdx);
       }
     }

     if(!candidates.empty()){
       const int pick =
         std::min(
           static_cast<int>(R::unif_rand() * static_cast<double>(candidates.size())),
           static_cast<int>(candidates.size()) - 1
         );

       result[syntheticIdx] = candidates[pick];
     }
   }

   return result;
 }

 static const double binaryRoundThreshold = 0.5;

 // Worker que reconstrui colunas binarias usando corte em 0.5
 struct BinaryRestoreWorker : public Worker {
   RMatrix<double> matrix;
   const RVector<int> columnIdx;

   BinaryRestoreWorker(NumericMatrix& matrix, const IntegerVector& columnIdx)
     : matrix(matrix), columnIdx(columnIdx){}

   void operator()(std::size_t begin, std::size_t end){
     const int nTargetCols = static_cast<int>(columnIdx.size());

     for(std::size_t rowIdx = begin; rowIdx < end; ++rowIdx){
       for(int idxPos = 0; idxPos < nTargetCols; ++idxPos){
         matrix(rowIdx, columnIdx[idxPos]) =
           (matrix(rowIdx, columnIdx[idxPos]) >= binaryRoundThreshold) ? 1.0 : 0.0;
       }
     }
   }
 };

 // Worker que arredonda colunas inteiras e, se preciso, corta em zero
 struct IntegerRestoreWorker : public Worker {
   RMatrix<double> matrix;
   const RVector<int> columnIdx;
   const bool clampZero;

   IntegerRestoreWorker(NumericMatrix& matrix,
                        const IntegerVector& columnIdx,
                        bool clampZero)
     : matrix(matrix), columnIdx(columnIdx), clampZero(clampZero){}

   void operator()(std::size_t begin, std::size_t end){
     const int nTargetCols = static_cast<int>(columnIdx.size());

     for(std::size_t rowIdx = begin; rowIdx < end; ++rowIdx){
       for(int idxPos = 0; idxPos < nTargetCols; ++idxPos){
         double value = std::round(matrix(rowIdx, columnIdx[idxPos]));

         if(clampZero && value < 0.0){
           value = 0.0;
         }

         matrix(rowIdx, columnIdx[idxPos]) = value;
       }
     }
   }
 };

 //' Restaura tipos de colunas apos desnormalizacao
 //'
 //' Primeiro arredonda colunas binarias, depois inteiras, e por fim aplica clamp
 //' em inteiras nao negativas
 //'
 //' @param synMat Matriz de sinteticos na escala original
 //' @param binCols Vetor 0-based com colunas binarias
 //' @param intCols Vetor 0-based com colunas inteiras
 //' @param nnIntCols Vetor 0-based com colunas inteiras nao negativas
 //'
 //' @keywords internal
 // [[Rcpp::export]]
 void RestoreTypesCpp(
     NumericMatrix& synMat,
     const IntegerVector& binCols,
     const IntegerVector& intCols,
     const IntegerVector& nnIntCols,
     const int numThreads
 ){

   const std::size_t nRows = static_cast<std::size_t>(synMat.nrow());

   ValidateColumnIndices(binCols, synMat.ncol(), "RestoreTypesCpp(binCols)");
   ValidateColumnIndices(intCols, synMat.ncol(), "RestoreTypesCpp(intCols)");
   ValidateColumnIndices(nnIntCols, synMat.ncol(), "RestoreTypesCpp(nnIntCols)");

   AbortIf(
     numThreads < 1, "RestoreTypesCpp: `numThreads` precisa ser pelo menos 1."
   );

   if(binCols.size() > 0){
     BinaryRestoreWorker worker(synMat, binCols);
     parallelFor(0, nRows, worker, 512, numThreads);
   }

   if(intCols.size() > 0){
     IntegerRestoreWorker worker(synMat, intCols, false);
     parallelFor(0, nRows, worker, 512, numThreads);
   }

   if(nnIntCols.size() > 0){
     IntegerRestoreWorker worker(synMat, nnIntCols, true);
     parallelFor(0, nRows, worker, 512, numThreads);
   }
 }

 // Worker que calcula a media de distancia por linha para o NearMiss
 struct AvgDistWorker : public Worker {
   const RMatrix<double> distanceMatrix;
   RVector<double> avgDistance;

   AvgDistWorker(const NumericMatrix& distanceMatrix, NumericVector& avgDistance)
     : distanceMatrix(distanceMatrix), avgDistance(avgDistance){}

   void operator()(std::size_t begin, std::size_t end){
     const int k = static_cast<int>(distanceMatrix.ncol());

     for(std::size_t rowIdx = begin; rowIdx < end; ++rowIdx){
       double sum = 0.0;

       for(int colIdx = 0; colIdx < k; ++colIdx){
         sum += distanceMatrix(rowIdx, colIdx);
       }

       avgDistance[rowIdx] = sum / k;
     }
   }
 };

 //' Calcula a media das distancias KNN por linha
 //'
 //' Essa rotina e usada pelo NearMiss-1 para ranquear observacoes majoritarias
 //'
 //' @param distMat Matriz de distancias retornada pelo HNSW
 //'
 //' @return Vetor com a media das distancias por linha
 //'
 //' @keywords internal
 // [[Rcpp::export]]
 NumericVector NearMissAvgDistCpp(
     const NumericMatrix& distMat,
     const int numThreads
 ){

   ValidateFiniteMatrix(distMat, "NearMissAvgDistCpp");

   AbortIf(
     distMat.ncol() <= 0, "NearMissAvgDistCpp: `distMat` precisa ter pelo menos uma coluna."
   );

   AbortIf(
     numThreads < 1, "NearMissAvgDistCpp: `numThreads` precisa ser pelo menos 1."
   );

   NumericVector avgDistance(distMat.nrow());

   if(distMat.nrow() == 0){
     return avgDistance;
   }

   AvgDistWorker worker(distMat, avgDistance);
   parallelFor(0, static_cast<std::size_t>(distMat.nrow()), worker, 512, numThreads);

   return avgDistance;
 }

