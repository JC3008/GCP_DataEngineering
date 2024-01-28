CREATE VIEW vendas_obt.analise_receita AS 
WITH 
-- Inserindo um surrogate key na tabela
sk as (
SELECT 
ROW_NUMBER() OVER (ORDER BY data_venda) as sk_venda,
* 
FROM `vendas-de-412318.vendas_obt.vendas_obt`

),
-- Receita por empresa
vendas_total as (
  SELECT 
  sk_venda,
  data_venda,
  empresa,
  SUM(preco * desconto) OVER(PARTITION BY empresa) as receita_por_empresa
  FROM sk
),

-- Percentual sobre receita clusterizado
percentual_sobre_total as (
  SELECT 
  v.sk_venda,
  v.data_venda,
  v.empresa,
  vt.receita_por_empresa,
  SUM(preco) OVER (PARTITION BY estado,v.empresa ) / vt.receita_por_empresa as percentual_receita_uf_sobre_total,
  SUM(preco) OVER (PARTITION BY canal_venda,v.empresa ) / vt.receita_por_empresa as percentual_receita_canal_venda_sobre_total,
  SUM(preco) OVER (PARTITION BY v.empresa,v.campanha ) / vt.receita_por_empresa as percentual_receita_campanha_sobre_total,
  SUM(quantidade) OVER (PARTITION BY genero,v.empresa,v.campanha ) as quantidade_vendida_genero_campanha

  FROM sk as v
  LEFT JOIN vendas_total as vt
  ON v.sk_venda = vt.sk_venda
)

-- obt enriquecida
SELECT  
vendas.*,
pt.receita_por_empresa,
pt.percentual_receita_uf_sobre_total,
pt.percentual_receita_canal_venda_sobre_total,
pt.percentual_receita_campanha_sobre_total,
pt.quantidade_vendida_genero_campanha

FROM sk as vendas
LEFT JOIN percentual_sobre_total as pt 
ON vendas.sk_venda = pt.sk_venda




