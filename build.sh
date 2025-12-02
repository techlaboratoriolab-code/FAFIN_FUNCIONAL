#!/usr/bin/env bash
# Script de build para o Render

set -o errexit

echo "🔧 Instalando dependências..."
pip install --upgrade pip
pip install -r requirements.txt

echo "✅ Build concluído com sucesso!"
