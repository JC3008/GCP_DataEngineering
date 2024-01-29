CREATE OR REPLACE TABLE vendas-de-412318.vendas_obt.analise_janeiro AS

WITH insere_sk_e_receita AS (
SELECT 
ROW_NUMBER() OVER (ORDER BY v.data_venda) AS sk_venda,

v.empresa,
v.estado,
v.idade,
v.curso,
v.canal_venda,
v.campanha,
v.data_venda,
v.quantidade * v.preco * (1 - v.desconto) as receita,
v.quantidade

FROM `vendas-de-412318.vendas_obt.vendas_janeiro` v
),

receita_clusters AS (

SELECT 
data_venda,
empresa,
quantidade,
SUM(receita) OVER (PARTITION BY empresa) as receita_empresa,
SUM(receita) OVER (PARTITION BY empresa, estado) as receita_estado,
SUM(receita) OVER (PARTITION BY estado, campanha, canal_venda) as receita_estado_campanha_canal,
receita,
estado,
campanha,
canal_venda,
curso
FROM insere_sk_e_receita
)

SELECT 
data_venda,
empresa,
estado,
campanha,
canal_venda,
quantidade,
curso,
receita_empresa,
receita_estado,
receita_estado_campanha_canal,
receita

FROM receita_clusters

ORDER By estado,campanha,canal_venda
