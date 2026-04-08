# Regras Operacionais

## Commit e Push Obrigatorios

Sempre que houver qualquer alteracao em um destes repositorios:

- `https://github.com/DevsAgregar/MR-Kommo-Sync-Leads.git`
- `https://github.com/DeckDev-RC/Sync-Leads-Kommo.git`

o fluxo obrigatorio apos concluir a alteracao e:

1. revisar o diff final;
2. criar commit com mensagem objetiva;
3. fazer `push` para os dois remotos/repositorios correspondentes;
4. so considerar a tarefa concluida depois que o `push` terminar sem erro.

## Regra de Comportamento

- Nao deixar alteracoes locais sem commit ao encerrar a tarefa.
- Nao parar em "alteracao pronta"; a entrega so termina com `commit` e `push`.
- Se houver falha de autenticacao, permissao, conflito ou rede, relatar claramente o bloqueio.
- Se o repositorio tiver dois remotos configurados para espelhamento, enviar para ambos.
- Se o trabalho afetar apenas um dos repositorios acima, ainda assim concluir com `commit` e `push` no repositorio alterado.

## Padrao de Mensagem

Usar mensagens curtas, tecnicas e objetivas, por exemplo:

- `fix: corrige sincronizacao de leads`
- `feat: adiciona regra de deduplicacao`
- `chore: ajusta configuracao de integracao`

## Definicao de Conclusao

Uma alteracao nesses repositorios so esta concluida quando:

- o codigo foi salvo;
- o commit foi criado;
- o push foi executado com sucesso.
