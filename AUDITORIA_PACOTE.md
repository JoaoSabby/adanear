# Auditoria técnica do pacote `sattvaR`

## Escopo da auditoria

Esta auditoria cobre:

- conformidade estrutural de pacote R;
- qualidade de implementação do `step_adanear()`;
- validações, testes e documentação;
- riscos de uso e recomendações práticas para evolução.

## 1) Estrutura de pacote R

Checklist de estrutura observado:

- `DESCRIPTION` presente e consistente com metadados, dependências e links.
- `NAMESPACE` gerado por `roxygen2` com export e métodos S3 esperados.
- Código R sob `R/` e código nativo sob `src/` com `Rcpp`/`RcppParallel`.
- Testes `testthat` organizados em `tests/testthat/`.
- Documentação em `man/` e vignette em `vignettes/`.

Status: **bom** para um pacote em estágio experimental.

## 2) Conformidade com padrões de step em `recipes`

O `step_adanear()` segue o contrato clássico de steps customizados:

- construtor (`step_adanear`),
- treino (`prep.step_adanear`),
- aplicação (`bake.step_adanear`),
- métodos auxiliares (`print`, `tidy`, `required_pkgs`).

Pontos positivos:

- `skip = TRUE` como padrão para evitar vazamento de dados em produção.
- seleção explícita da variável-alvo e validação de papel "outcome".
- persistência no objeto treinado de estado necessário ao `bake`.

## 3) Qualidade funcional do pipeline ADASYN + NearMiss

### Fluxo de entrada e validações

Antes de balancear, o step exige:

- alvo fator binário, sem `NA` e sem empate de classes;
- preditores numéricos, sem `NA` e finitos;
- pelo menos um preditor;
- desbalanceamento real no treino.

Isso reduz risco de erro silencioso e facilita diagnóstico.

### Fluxo algorítmico

1. Normaliza preditores com média/desvio do treino.
2. Gera sintéticos da minoria (variação ADASYN).
3. Concatena dados originais + sintéticos.
4. Recalcula matriz normalizada do conjunto aumentado.
5. Seleciona maioria por NearMiss-1 contra minoria.
6. Restaura tipos e ordem de colunas de entrada.

## 4) Qualidade de implementação C++

Pontos fortes:

- validações defensivas em C++ (dimensão, finitude, índices e binariedade);
- paralelização com `RcppParallel` nos kernels de normalização/interpolação;
- mensagens de erro explícitas para contratos inválidos.

Risco residual:

- sem benchmark automatizado versionado no CI (há `bench` em `Suggests`, mas não há rotina formal no repositório).

## 5) Testes

Cobertura existente (boa para o estágio):

- validações de preditores e viabilidade ADASYN;
- comportamento `prep` e estado aprendido;
- comportamento de `bake` com/sem aumento;
- reprodutibilidade por seed;
- preservação de nomes/tipos de colunas;
- `tidy()` retornando estrutura esperada.

Lacunas recomendadas:

- casos extremos de cardinalidade alta e dados muito grandes;
- testes de robustez com `integer`/`logical` binário misturado com `double`;
- regressão para estabilidade entre plataformas (Linux/Mac/Windows).

## 6) Documentação e usabilidade

Pontos positivos:

- README claro sobre objetivo e método principal;
- documentação do step com parâmetros e contrato de uso.

Melhorias:

- incluir seção "troubleshooting" com erros comuns e correções;
- adicionar exemplo de pipeline completo com `tune` e validação cruzada;
- publicar matriz de compatibilidade de versões (`recipes`, `RcppHNSW`, `R`).

## 7) Resumo em tópicos do `step_adanear` (entrada -> processamento -> saída)

### Como os dados chegam

- Entram no `prep()` como `training` do recipe.
- O usuário informa a coluna-alvo no `...`.
- O step identifica automaticamente preditores com papel `predictor`.
- No `bake()`, chega `new_data` com mesmas colunas esperadas.

### Passos de processamento de balanceamento

- valida contrato do alvo e dos preditores;
- normaliza preditores com estatísticas salvas no `prep`;
- executa ADASYN para gerar amostras sintéticas da classe minoritária;
- junta originais com sintéticos;
- executa NearMiss-1 para subamostrar a classe majoritária;
- recompõe tipos (fator/integer/binário) e layout das colunas originais.

### Como os dados saem

- Saem como `tibble`.
- Mantêm as colunas originais (mesmos nomes e tipos esperados).
- Retornam com distribuição de classes mais equilibrada.
- O step deve atuar só no treino quando `skip = TRUE`.

## 8) Avaliação final

**Maturidade atual: boa base técnica para pacote experimental**, com foco correto em:

- validação forte,
- integração ao ecossistema `recipes`,
- desempenho via C++/paralelização,
- reprodutibilidade via `seed`.

Para elevar a padrão CRAN/produção mais rígido:

1. adicionar CI com `R CMD check` multi-versão e multi-OS;
2. expandir suíte de testes para stress/edge cases;
3. reforçar documentação operacional e de troubleshooting.
