# Tatimr - Espelho do Banco Clinica Agil

Esta pasta contem um espelho isolado do subsistema de banco da Clinica Agil extraido de:

`C:\Users\User\Desktop\notafiscalmirela`

Nada foi alterado no projeto de origem. O que foi espelhado aqui:

- `login.py`: fluxo HTTP da Clinica Agil com:
  - autenticacao por HTTP
  - coleta da planilha de pacientes
  - extracao dos registros XLSX
  - sincronizacao incremental em SQLite
  - versionamento de linhas
  - reconstrucao das tabelas curadas
- `mirella_pacientes.sqlite3`: copia local do banco incremental existente no projeto original

## Estrutura

- `login.py`
- `mirella_pacientes.sqlite3`
- `.env.example`
- `requirements.txt`

## Dependencias

```powershell
py -3 -m pip install -r requirements.txt
```

## Variaveis de ambiente

Copie `.env.example` para `.env` e ajuste se quiser sobrescrever os padroes do script.

## Exemplos de uso

Sincronizar apenas pacientes:

```powershell
py -3 login.py --somente pacientes --sem-input --data-pacientes-de 01/01/2024 --data-pacientes-ate 31/01/2024
```

Reconstruir as tabelas curadas a partir de `patients_latest`:

```powershell
py -3 login.py --rebuild-curated
```

## Observacoes

- O banco SQLite e local e pode conter dados reais.
- O script cria saidas em `exports/` quando exporta arquivos.
- Se voce quiser reprocessar tudo desde o inicio, use `--reprocessar-pacientes`.
