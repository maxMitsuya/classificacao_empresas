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
