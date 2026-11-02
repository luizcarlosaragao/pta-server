# Practical Exam
Please **fork** this repo and **develop your server** based on the protocol described in [pta.pdf][ptad] file (in portuguese). Before coding, I suggest you carefully read the document spec and understand the protocol. Experiment the protocol, using paper and pencil, drawing examples of protocol usage.

Please develop your server code in the directory [pta-server][ptas]. The user list that must be accepted by the protocol must be read from the [pta-server/users.txt][ptau] file. The files to be served by your server are in the [pta-server/files][ptaa] directory. Please don't change these files and directories.

Your server code will be evaluated through an automatic testing tool. This way, in order to test your code you can use the [pta-client.py][ptac] file. This file makes some tests but it is not an extensive test. So, keep in mind that the actual evaluation will test other aspects of your protocol implementation.

You could use **any programming language**. But you must provide instructions in order to successfully run your code. Describe all stuff about libraries, interpreters, versions and so on. After finishing development, you must share the link of your github repo in the evaluation environment ([sigaa.ufpa.br](http://sigaa.ufpa.br/)).

[ptas]: <https://github.com/glaucogoncalves/pta/tree/master/pta-server>
[ptau]: <https://github.com/glaucogoncalves/pta/tree/master/pta-server/users.txt>
[ptaa]: <https://github.com/glaucogoncalves/pta/tree/master/pta-server/files>
[ptac]: <https://github.com/glaucogoncalves/pta/tree/master/pta-client.py>
[ptad]: <https://github.com/glaucogoncalves/pta/tree/master/pta.pdf>

---

# Servidor PTA implementado

Servidor de transferência de arquivos sobre **TCP**, conforme `pta.pdf`.
Implementado em Python com `asyncio`, sem dependências externas, frontend,
HTTP ou banco de dados. A especificação e as instruções originais acima foram
preservadas, assim como `pta-client.py`, `pta.pdf`, `pta-server/users.txt` e
todos os arquivos em `pta-server/files/`.

## Requisitos e execução

- Python **3.11 ou superior**; biblioteca padrão apenas, sem `pip install`.
- Linux é a plataforma principal de execução e validação.
- Porta TCP **11550** disponível. O servidor escuta em **0.0.0.0** por padrão.

Na raiz do repositório:

```bash
python3 pta-server/server.py
```

Para limitar o acesso ao computador local ou escolher outra porta:

```bash
python3 pta-server/server.py --host 127.0.0.1 --port 11550
python3 pta-server/server.py --help
```

Também é possível iniciar de qualquer diretório usando o caminho absoluto do
script. Os caminhos dos usuários e arquivos são relativos ao próprio código,
não ao diretório do terminal. A apresentação exige um usuário de `users.txt`
(`user1`, `user2` ou `user3` nos materiais originais), com comparação sensível a
maiúsculas e minúsculas. A lista é carregada na inicialização; alterações exigem
reinício. Problemas de configuração ou porta ocupada encerram o processo com
erro e diagnóstico em `stderr`. Logs nunca são enviados pelo protocolo.

Ctrl+C (SIGINT) e SIGTERM encerram o listener e as conexões ativas.

## Mensagens e estados

Cada resposta repete exatamente o campo de sequência recebido. O servidor não
exige início em zero nem rejeita saltos/repetições: a especificação atribui o
incremento ao cliente. A sequência aceita contém somente dígitos ASCII, sem
conversão para um inteiro de tamanho fixo.

| Pedido | Resposta | Estado da conexão |
| --- | --- | --- |
| `73 CUMP user1` | `73 OK` | Pronto |
| `73 CUMP desconhecido` | `73 NOK` | Fecha |
| Qualquer outro pedido antes de CUMP válido | `SEQ NOK` | Fecha |
| `74 LIST` | `74 ARQS N nome1,nome2,...` | Mantém |
| `75 PEGA nome1` | `75 ARQ B ` seguido de exatamente B bytes | Mantém |
| Arquivo ausente, inacessível ou caminho proibido | `SEQ NOK` | Mantém |
| Comando inválido ou novo CUMP no estado pronto | `SEQ NOK` | Mantém |
| `76 TERM` no estado pronto | `76 OK` | Fecha |

Os campos são separados por um único espaço ASCII. Comandos distinguem
maiúsculas/minúsculas; LIST e TERM não recebem argumentos. CUMP recebe um
argumento. PEGA atende somente o primeiro arquivo e ignora argumentos de
arquivos adicionais, conforme a regra específica do PDF.

LIST retorna nomes ordenados, separados por vírgulas, sem vírgula final. Um
diretório vazio produz `SEQ ARQS 0 `, incluindo o espaço final. A listagem
considera arquivos regulares diretamente no diretório; subdiretórios, links e
nomes não representáveis no protocolo (não ASCII, espaços ou vírgulas) não são
listados nem servidos. Caminhos absolutos, separadores de diretórios e `..`
como componente de navegação são recusados. No Linux, a abertura usa
`O_NOFOLLOW` para impedir substituição por link simbólico e `O_NONBLOCK` para
não bloquear caso o nome seja substituído por um FIFO.

O arquivo é aberto em modo binário e seu tamanho é obtido pelo descritor aberto.
A transferência usa blocos de até 64 KiB e controle de fluxo do socket. Nenhum
byte do arquivo é decodificado, transformado ou acrescido; arquivos vazios
retornam `SEQ ARQ 0 `. Se uma leitura falhar após o cabeçalho, a conexão fecha
sem inserir NOK no corpo. Os arquivos servidos devem permanecer estáveis
durante uma transferência; não há snapshot contra edição externa simultânea.

## Delimitação TCP e ambiguidades da especificação

TCP entrega um fluxo de bytes, sem preservar os limites dos envios do cliente.
O PDF não define terminador nem comprimento das requisições, e o cliente de
exemplo não acrescenta quebra de linha. Por isso, não é possível garantir a
identificação do fim de uma mensagem sem uma convenção adicional.

Esta implementação mantém um buffer por conexão e aceita:

1. **LF ou CRLF** no fim de cada requisição, permitindo delimitação explícita.
2. **Ausência de terminador**, como no cliente original: após receber bytes,
   aguarda **100 ms sem novos dados** para interpretar a requisição acumulada.

O intervalo pode ser ajustado:

```bash
python3 pta-server/server.py --legacy-idle-ms 250
```

Fragmentos separados por pausas maiores que o intervalo podem ser interpretados
como mensagens distintas. Comandos concatenados sem delimitador não são
suportados. Clientes devem aguardar a resposta completa antes de enviar o
próximo pedido. Mensagens recebidas enquanto LIST ou PEGA está em andamento,
incluindo pedidos já concatenados atrás da operação no buffer de entrada, são
descartadas; não há fila de operações por conexão. O término da operação é o
término do envio pelo servidor, não uma confirmação de consumo pelo cliente.
Respostas não recebem LF/CRLF ou qualquer outro terminador extra.

Requisições são limitadas a **64 KiB**. Ao ultrapassar o limite, o servidor
responde NOK quando possível e fecha a conexão. Se uma mensagem malformada
não contém sequência numérica recuperável, responde `0 NOK`; essa convenção
não é definida pelo PDF. Erros normais de formato preservam uma sessão já
autenticada; antes da autenticação, encerram a conexão.

O texto do PDF determina fechamento após apresentação inválida; essa regra
prevalece sobre a seta simplificada de retorno ao estado inicial do diagrama.

## Testes automatizados

Na raiz do repositório:

```bash
python3 -m unittest discover -s pta-server/tests -v
```

Os testes iniciam servidores em `127.0.0.1` com portas temporárias escolhidas
pelo sistema operacional. Não é necessário iniciar o servidor manualmente.
O ambiente deve permitir sockets TCP locais. Dados auxiliares e downloads são
gravados em diretórios temporários, sem modificar os materiais do professor.

A suíte verifica apresentação, estados, sequências, mensagens inválidas,
fragmentação, LF/CRLF e modo sem terminador, limites de buffer, listagens
maiores que 2048 bytes, arquivos ausentes, bloqueio de caminhos/links,
falhas de armazenamento, truncamento durante envio, desconexão, encerramento
por sinal, leitura lenta, comandos durante transferência e **20 clientes
simultâneos**. Todos os **16 arquivos originais**, incluindo PNG e arquivos
vazios, são baixados e comparados por tamanho, conteúdo e SHA-256.

O workflow `.github/workflows/tests.yml` executa a suíte no Ubuntu com
**Python 3.11 e 3.14**, em pushes e pull requests.

### Cliente original do professor

Com o servidor em execução, pode-se executar o cliente original pelo caminho
absoluto, a partir de um diretório temporário, para evitar downloads na raiz:

```bash
PTA_REPO="$(pwd)"
PTA_DOWNLOADS="$(mktemp -d)"
cd "$PTA_DOWNLOADS"
python3 "$PTA_REPO/pta-client.py" 127.0.0.1 11550 user1
```

Esse cliente faz uma seleção aleatória de arquivo e tem limitações: decodifica
dados como texto UTF-8, grava downloads em modo texto e sua contagem de arquivos
depende da divisão dos dados entre leituras TCP. Portanto, pode falhar ao
escolher o PNG ou produzir resultados inconsistentes quando a listagem é
fragmentada. Ele não é um verificador suficiente da integridade do servidor.

A suíte executa o cliente original sem modificá-lo, fixando externamente a
escolha de um arquivo textual de 2048 bytes e verificando **6/6**, TERM e o
conteúdo baixado. Os downloads binários e vazios são verificados pelo cliente
de testes próprio, que lê bytes e respeita o tamanho anunciado pelo servidor.

## Entrega

Repositório de entrega: **https://github.com/luizcarlosaragao/pta**, fork público
do repositório original, mantendo seu histórico. Para obter e iniciar:

```bash
git clone https://github.com/luizcarlosaragao/pta.git
cd pta
python3 pta-server/server.py
```

O link a entregar no SIGAA é a página do fork acima.
A aprovação nos testes locais não garante aprovação em testes ocultos,
especialmente nas ambiguidades do protocolo documentadas acima.
