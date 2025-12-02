# 🏥 Sistema de Processamento de Lotes LAB

Sistema completo para processamento multi-etapas de imagens médicas com integração AWS S3 e envio para Orizon TISS.

## 📋 Funcionalidades

- **Etapa 1:** Análise e filtragem de requisições de imagens do banco
- **Etapa 2:** Download automatizado de imagens do AWS S3
- **Etapa 3:** Processamento e conversão de formatos de imagem
- **Etapa 4:** Envio de anexos para sistema Orizon TISS

## 🚀 Deploy no Render

### ⚙️ Configurações Manuais Necessárias

**1. No Render Dashboard, vá em Settings:**

**Environment:**
- `PYTHON_VERSION` = `3.11.9` (IMPORTANTE!)

**Build & Deploy:**
- **Build Command:** `pip install --upgrade pip && pip install -r requirements.txt`
- **Start Command:** `gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --timeout 300`

**Health Check:**
- **Health Check Path:** `/`
- **Health Check Timeout:** `30` segundos
- **Health Check Interval:** `60` segundos
- **Health Check Grace Period:** `300` segundos (5 minutos)

### 🔐 Variáveis de Ambiente

Configure no **Environment** do Render:

**Secrets (marcar como Secret):**
- `AWS_ACCESS_KEY` - Chave de acesso AWS
- `AWS_SECRET_KEY` - Chave secreta AWS
- `ORIZON_LOGIN` - Login webservice Orizon
- `ORIZON_SENHA` - Senha MD5 Orizon

**Públicas:**
- `AWS_REGION` = `sa-east-1`
- `S3_BUCKET_NAME` = `aplis2`
- `ORIZON_REGISTRO_ANS` = `005711`

### 📦 Instância Recomendada

- **Free Tier:** Para testes (limitações)
- **Starter ($7/mês):** Para produção
  - Sem sleep após inatividade
  - Build mais rápido
  - Melhor performance

## 🔧 Desenvolvimento Local

```bash
# Instalar dependências
pip install -r requirements.txt

# Configurar ambiente
cp .env.example .env
# Editar .env com suas credenciais

# Executar
python app.py
```

Acesse: http://localhost:5000

## 📁 Estrutura do Projeto

```
projeto/
├── app.py                      # Aplicação Flask principal
├── unificado_v1.py             # Script de processamento de lotes
├── enviar_anexos_producao.py   # Módulo de envio Orizon TISS
├── templates/
│   └── index.html              # Interface web
├── uploads/                    # Pasta para arquivos temporários
├── requirements.txt            # Dependências Python
├── Procfile                    # Comando de start
├── build.sh                    # Script de build
├── runtime.txt                 # Versão Python (3.11.9)
└── .env.example                # Exemplo de variáveis

## 🛠️ Tecnologias

- **Flask 3.0** - Framework web
- **Pandas 2.3+** - Manipulação de dados
- **Boto3** - AWS SDK Python
- **lxml 6.0+** - Processamento XML
- **Pillow 12.0+** - Processamento de imagens
- **Gunicorn** - WSGI HTTP Server

## ⚠️ Notas Importantes

1. **Python 3.11.9 obrigatório** - Compatibilidade com Pillow
2. **Timeout 300s** - Processos podem ser longos
3. **1 Worker** - Evita conflitos em arquivos temporários
4. **Health Check Grace Period 300s** - App precisa de tempo para iniciar

## 📞 Troubleshooting

### Build falha no Render
- Verificar se Python 3.11.9 está configurado
- Limpar build cache e tentar novamente

### Timeout no deploy
- Aumentar Health Check Grace Period para 300s
- Verificar logs do aplicativo

### Erro ao importar módulos
- Confirmar que todos os arquivos estão no repositório
- Verificar requirements.txt

---

**Desenvolvido para LAB - Medicina Diagnóstica**
