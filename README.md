# Categorização de Empresas Brasileiras - Good Places to Work

Este projeto realiza um processo completo de ETL (Extração, Transformação e Carga) para classificar empresas brasileiras segundo seus CNAEs fiscais, visando identificar potenciais "Good Places to Work". Desenvolvido no Google Colab, utiliza PySpark para processamento distribuído de grandes volumes de dados da Receita Federal.

## Objetivo

Classificar todas as empresas do Brasil de acordo com seu CNAE fiscal, permitindo:
- Identificação de setores promissores para trabalho
- Análise de distribuição geográfica de empresas por segmento
- Categorização de empresas por porte e capital social
- Integração com dados do Simples Nacional

## Fonte dos Dados

Os dados são obtidos diretamente do Banco de Dados da Receita Federal:
[Arquivos CNPJ Receita Federal](https://arquivos.receitafederal.gov.br/cnpj/)

Os arquivos estão organizados por data no formato "ano-mês" (ex: `2023-10` para outubro de 2023).

## Funcionalidades Principais

- **Web Scraping Automatizado**: Download dos arquivos mais recentes diretamente do site da Receita Federal
- **Processamento Distribuído**: Utilização do PySpark para lidar com grandes volumes de dados
- **Classificação por CNAE**: Mapeamento de mais de 40 categorias de negócios
- **Integração de Dados**: Combinação de informações de empresas, estabelecimentos e sócios
- **Otimização de Memória**: Técnicas avançadas para processamento eficiente

## Como Executar no Google Colab

1. Acesse o notebook no Google Colab através do link:
   [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1a2TOYPvEoi4SOlzM4kIvnuYZUYmFyX1s)

2. Monte seu Google Drive quando solicitado para armazenar os dados

3. Execute as células sequencialmente:
   - Configuração inicial e instalação de dependências
   - Download dos dados da Receita Federal
   - Processamento e transformação dos dados
   - Classificação por CNAE
   - Geração dos resultados finais

4. Os arquivos processados serão salvos no seu Google Drive na pasta `arquivos_cnpj`

## Estrutura do Código

### 1. Coleta de Dados
- Web scraping dos arquivos CNPJ mais recentes
- Download paralelizado para melhor performance
- Conexão persistente com Google Drive

```python
# Manter conexão ativa com Google Drive
from google.colab import output
output.eval_js('google.colab.kernel.proxyPort(5000)')

# Download paralelizado dos arquivos
with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(lambda args: download_file(*args), file_urls)
```
### 2. Processamento de Dados
- Configuração otimizada do Spark para big data
- Schemas tipados para cada categoria de arquivo
- Funções auxiliares para operações complexas
- Gerenciamento eficiente de memória

```python
# Configuração do Spark para grandes volumes
spark = SparkSession.builder \
    .config("spark.driver.memory", "16g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Schema para arquivo de empresas
empresas_header = ['CNPJ_Basico', 'Razao_Social', 'Natureza_Juridica', ...]
schema_empresas = StructType([
    StructField('CNPJ_Basico', StringType()),
    StructField('Razao_Social', StringType()),
    ...
])

# Função para joins otimizados
def optimized_join(df_left, df_right, join_col):
    return df_left.join(broadcast(df_right), join_col, 'left')
```
### 3. Transformação e Classificação
- Limpeza e padronização de dados brutos
- Conversão de formatos e tipos de dados
- Enriquecimento com tabelas de referência
- Mapeamento completo de CNAEs para categorias

```python
df = df.na.fill({
    'Capital_Social': 0,
    'Porte': 'NÃO INFORMADO',
    'Municipio': 'NÃO CADASTRADO'
})

# Conversão de capital social para float
df = df.withColumn(
    'Capital_Social',
    regexp_replace(
        regexp_replace(col('Capital_Social'), '\\.', ''),
        ',', '.').cast(FloatType())
)

# Join com tabela de CNAEs
df = df.join(
    broadcast(df_cnae),
    df['Cnae_Fiscal_Principal'] == df_cnae['Codigo'],
    'left'
)

# Classificação por segmento
cnae_map = {
    '62': 'Tecnologia',
    '41': 'Construção',
    '86': 'Saúde'
}

df = df.withColumn(
    'Segmento',
    when(col('Cnae_Fiscal_Principal').isin(cnae_map.keys()), 
        col('Cnae_Fiscal_Principal')).otherwise('Outros')
)
```
### 4. Análise e Persistência
- Agregações estatísticas por categoria
- Amostragem para validação
- Persistência em formato otimizado
- Visualização de resultados

```python
# Análise por segmento e porte
df_analytics = df.groupBy('Segmento', 'Porte').agg(
    count('*').alias('Qtd_Empresas'),
    avg('Capital_Social').alias('Capital_Medio')
)

# Persistência em Parquet particionado
df.write.partitionBy('Segmento') \
    .mode('overwrite') \
    .parquet('/content/drive/MyDrive/cnpj_processed')

# Visualização de amostra
display(df_analytics.orderBy('Qtd_Empresas', ascending=False).limit(10))
```
### Requisitos
O projeto foi desenvolvido para rodar no Google Colab e inclui automaticamente todas as dependências necessárias:
- Python 3.10+
- PySpark 3.4+
- BeautifulSoup 4.0+
- Requests 2.0+

### Limitações
- Processamento completo requer ambiente com pelo menos 16GB de RAM
- Download inicial dos arquivos pode demorar várias horas
- Dados da Receita Federal são atualizados mensalmente

### Contribuição
Contribuições são bem-vindas! Sinta-se à vontade para:
- Reportar issues
- Sugerir melhorias no mapeamento de CNAEs
- Otimizações no processamento
