def limpar_pasta_temporaria():
    """Remove todos os arquivos da pasta temporária usada para downloads."""
    temp_dir = get_temp_dir()
    if temp_dir.exists() and temp_dir.is_dir():
        for item in temp_dir.iterdir():
            try:
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            except Exception as e:
                print(f"[WARN] Não foi possível remover {item}: {e}")
        print(f"[INFO] Pasta temporária limpa: {temp_dir}")
    else:
        print(f"[INFO] Pasta temporária não existe: {temp_dir}")
import pandas as pd
import os
import webbrowser
import py7zr
import csv
import shutil
import sys
from PIL import Image
import zipfile
import glob
import requests
import json
import time
from collections import defaultdict
import boto3
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import requests
import re
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from PyPDF2 import PdfReader, PdfWriter
import tempfile
from dotenv import load_dotenv


#######################################
## Filtra informações por tipo e combinação
#######################################

def _filtrar_por_tipo_e_combinacao(df_imagens_filtrado):
    # Coleta todas as requisições únicas que contêm pelo menos um dos tipos 1, 15 ou 16.
    requisicoes_com_algum_tipo = set(df_imagens_filtrado['CodRequisicao_extraido'].unique())
    
    # Coleta as requisições que têm a combinação completa (tipo 1, e 15 ou 16)
    requisicoes_com_tipo_1 = set(df_imagens_filtrado[df_imagens_filtrado['Tipo'] == 1]['CodRequisicao_extraido'])
    requisicoes_com_tipo_15_ou_16 = set(df_imagens_filtrado[df_imagens_filtrado['Tipo'].isin([15, 16])]['CodRequisicao_extraido'])
    requisicoes_completas = requisicoes_com_tipo_1.intersection(requisicoes_com_tipo_15_ou_16)

    # Identifica as requisições incompletas
    requisicoes_incompletas = requisicoes_com_algum_tipo - requisicoes_completas

    # Filtra o DataFrame para obter as requisições completas
    df_final_completo = df_imagens_filtrado[df_imagens_filtrado['CodRequisicao_extraido'].isin(requisicoes_completas)]
    
    # Filtra o DataFrame para obter as requisições incompletas
    df_final_incompleto = df_imagens_filtrado[df_imagens_filtrado['CodRequisicao_extraido'].isin(requisicoes_incompletas)]

    return df_final_completo, df_final_incompleto

def etapa_1_filtrar_requisicoes(lote_digitado, is_web_mode):
    """Filtra as requisições, gera os arquivos de completos e incompletos, e PAUSA para decisão do usuário."""
    print("="*50)
    print("ETAPA 1: Filtrando requisições...")
    print("="*50)

    # Criar pasta temporária para arquivos CSV
    pasta_temp = tempfile.mkdtemp()
    print(f"📁 Pasta temporária criada: {pasta_temp}")

    # Salvar o caminho da pasta temporária em um arquivo para o app.py usar
    base_dir = Path(__file__).resolve().parent
    temp_path_file = base_dir / 'temp_path.txt'
    with open(temp_path_file, 'w') as f:
        f.write(pasta_temp)

    try:
        df = pd.read_csv(r"C:\Users\supor\Desktop\Lista\requisicao.csv", dtype={'CodRequisicao': str})
    except FileNotFoundError:
        print("Erro: O arquivo 'requisicao.csv' não foi encontrado.")
        return
    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo: {e}")
        return

    try:
        # Converte a coluna 'Lote' do DataFrame para numérica, tratando erros
        df['Lote'] = pd.to_numeric(df['Lote'], errors='coerce')

        # Filtra o DataFrame onde a coluna 'Lote' é igual ao número digitado
        df_filtrado = df[df['Lote'] == lote_digitado]
    except ValueError:
        print("Entrada inválida. Por favor, digite um número inteiro para o lote.")
        return
    except Exception as e:
        print(f"Ocorreu um erro durante o filtro: {e}")
        return

    requisicoes = df_filtrado['CodRequisicao']
    caminho_salvar = r"C:\Users\supor\Desktop\sqllite\requisicoes_filtradas.csv"
    requisicoes.to_csv(caminho_salvar, index=False, header=False)
    print(f"Os códigos de requisição filtrados foram salvos com sucesso em: {caminho_salvar}")

    caminho_imagem = r"C:\Users\supor\Desktop\Lista\requisicaoimagem.csv"
    try:
        requisicoes_filtradas_df = pd.read_csv(caminho_salvar, header=None, names=['CodRequisicao'], dtype={'CodRequisicao': str})
        df_imagem = pd.read_csv(caminho_imagem)

        df_imagem['CodRequisicao_extraido'] = df_imagem['NomArquivo'].astype(str).str.split('_').str[0]
        df_imagem_filtrado = pd.merge(df_imagem, requisicoes_filtradas_df, left_on='CodRequisicao_extraido', right_on='CodRequisicao', how='inner')
        df_imagem_filtrado = df_imagem_filtrado.drop(columns=['CodRequisicao'])

        caminho_salvar_imagem = r"C:\Users\supor\Desktop\Lista\requisicaoimagem_filtrada.csv"
        tipos_a_filtrar = [1, 15, 16]
        df_imagem_filtrado_por_tipo = df_imagem_filtrado[df_imagem_filtrado['Tipo'].isin(tipos_a_filtrar)].copy()
        df_imagem_filtrado_por_tipo.to_csv(caminho_salvar_imagem, index=False)
        print(f"O arquivo de imagens filtrado foi salvo com sucesso em: {caminho_salvar_imagem}")

        df_imagem_intermediario = pd.read_csv(caminho_salvar_imagem)
        df_imagem_intermediario['CodRequisicao_extraido'] = df_imagem_intermediario['NomArquivo'].astype(str).str.split('_').str[0]

        # FILTRAGEM POR TIPO E COMBINAÇÃO - PONTO DE PAUSA
        print("\n" + "="*50)
        print("🔍 FILTRANDO POR TIPO E COMBINAÇÃO...")
        print("="*50)

        df_finalmente_filtrado_completo, df_finalmente_filtrado_incompleto = _filtrar_por_tipo_e_combinacao(df_imagem_intermediario)

        # Usar pasta temporária para salvar arquivos
        caminho_salvar_final_completo = os.path.join(pasta_temp, "requisicaoimagem_filtrada_final.csv")
        df_finalmente_filtrado_completo.to_csv(caminho_salvar_final_completo, index=False)
        print(f"✅ Arquivo de requisições COMPLETAS salvo: {caminho_salvar_final_completo}")
        print(f"   Total de registros completos: {len(df_finalmente_filtrado_completo)}")
        print(f"   Requisições únicas completas: {df_finalmente_filtrado_completo['CodRequisicao_extraido'].nunique()}")

        # Salva o arquivo de requisições incompletas na pasta temporária
        caminho_salvar_final_incompleto = os.path.join(pasta_temp, "requisicaoimagem_filtrada_incompleta.csv")
        df_finalmente_filtrado_incompleto.to_csv(caminho_salvar_final_incompleto, index=False)
        print(f"\n⚠️  Arquivo de requisições INCOMPLETAS salvo: {caminho_salvar_final_incompleto}")
        print(f"   Total de registros incompletos: {len(df_finalmente_filtrado_incompleto)}")
        print(f"   Requisições únicas incompletas: {df_finalmente_filtrado_incompleto['CodRequisicao_extraido'].nunique()}")

    except FileNotFoundError:
        print("Erro: Um dos arquivos ('requisicoes_filtradas.csv' ou 'requisicaoimagem.csv') não foi encontrado.")
        return
    except Exception as e:
        print(f"Ocorreu um erro ao ler um dos arquivos: {e}")
        return

    # --- SEMPRE PAUSAR AQUI PARA MOSTRAR RESULTADO DA FILTRAGEM ---
    print("\n" + "="*50)
    print("⏸️  ETAPA 1 CONCLUÍDA - PAUSA PARA VERIFICAÇÃO")
    print("="*50)

    # Caminho do arquivo de incompletos na pasta temporária
    caminho_incompletos = os.path.join(pasta_temp, "requisicaoimagem_filtrada_incompleta.csv")
    tem_incompletos = False

    if os.path.exists(caminho_incompletos):
        try:
            df_incompletos = pd.read_csv(caminho_incompletos)
            tem_incompletos = not df_incompletos.empty
        except:
            tem_incompletos = False

    if tem_incompletos:
        print(f"⚠️  ATENÇÃO: {len(df_incompletos)} requisições incompletas encontradas!")
        print(f"📋 Total de requisições únicas incompletas: {df_incompletos['CodRequisicao_extraido'].nunique()}")

        if is_web_mode:
            print("\n👉 Verifique os dados no navegador e decida se deseja continuar.")
            print("   O processo aguarda sua decisão...")
            sys.exit(0)
        else:
            # Modo console - aguardar input sem gerar HTML
            print("\n" + "="*50)
            while True:
                continuar = input("❓ Deseja continuar com o processo mesmo assim? (s/n): ").lower()
                if continuar in ['s', 'n']:
                    break
                print("⚠️  Resposta inválida. Por favor, digite 's' para sim ou 'n' para não.")

            if continuar == 'n':
                print("\n❌ Processo interrompido pelo usuário.")
                sys.exit(1)
            else:
                print("\n✅ Continuando o processo...")
    else:
        print("✅ Nenhuma requisição incompleta encontrada!")
        print("   Todas as requisições possuem a combinação necessária (Tipo 1 E Tipo 15 ou 16).")

        if is_web_mode:
            print("\n👉 Processo pausado. Verifique os dados no navegador e confirme para continuar.")
            sys.exit(0)
        else:
            # Modo console - perguntar se quer continuar mesmo sem incompletos
            print("\n" + "="*50)
            while True:
                continuar = input("❓ Deseja prosseguir com o download e processamento? (s/n): ").lower()
                if continuar in ['s', 'n']:
                    break
                print("⚠️  Resposta inválida. Por favor, digite 's' para sim ou 'n' para não.")

            if continuar == 'n':
                print("\n❌ Processo interrompido pelo usuário.")
                sys.exit(1)
            else:
                print("\n✅ Continuando o processo...")

    # Se chegou aqui no modo console, significa que o usuário confirmou continuar
            
#######################################
## Baixar imagems S3
#######################################

# Carregar variáveis do .env
dotenv_path = Path(__file__).resolve().parent / '.env'
if dotenv_path.exists():
    print(f"Carregando variáveis de ambiente de: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)
else:
    print(f"Aviso: Arquivo .env não encontrado em {dotenv_path}. O script pode falhar se as variáveis de ambiente não estiverem definidas.")

# --- Configurações AWS S3 (lidas do .env) ---
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'sa-east-1')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'aplis2')
IMAGE_PREFIX = 'lab/Arquivos/Foto/'

# Caminhos

# Usar pasta temporária para salvar arquivos baixados
def get_temp_dir():
    base_dir = Path(__file__).resolve().parent
    temp_path_file = base_dir / 'temp_path.txt'
    if temp_path_file.exists():
        with open(temp_path_file, 'r') as f:
            pasta_temp = f.read().strip()
        return Path(pasta_temp)
    else:
        return Path(r"C:\Users\supor\AppData\Local\Temp\unificado_temp")

DESTINO_IMAGENS = get_temp_dir()
DESTINO_LAUDOS = get_temp_dir()

def obter_caminho_processar_csv():
    """Lê o caminho da pasta temporária e retorna o caminho do arquivo processar.csv"""
    base_dir = Path(__file__).resolve().parent
    temp_path_file = base_dir / 'temp_path.txt'

    if temp_path_file.exists():
        with open(temp_path_file, 'r') as f:
            pasta_temp = f.read().strip()
        return os.path.join(pasta_temp, 'processar.csv')
    else:
        # Fallback para caminho antigo
        return r"C:\Users\supor\Desktop\Lista\processar.csv"

# ETAPA 1: Mapeamento de prefixos para IMAGENS
PREFIXOS_IMAGENS = {
    '0200': 'lab/Arquivos/Foto/0200/',
    '0031': 'lab/Arquivos/Foto/0031/',
    '0032': 'lab/Arquivos/Foto/0032/',
    '0040': 'lab/Arquivos/Foto/0040/',
    '0049': 'lab/Arquivos/Foto/0049/',
    '0085': 'lab/Arquivos/Foto/0085/',
    '0100': 'lab/Arquivos/Foto/0100/',
    '0101': 'lab/Arquivos/Foto/0101/',
    '0102': 'lab/Arquivos/Foto/0102/',
    '0103': 'lab/Arquivos/Foto/0103/',
    '0300': 'lab/Arquivos/Foto/0300/',
    '8511': 'lab/Arquivos/Foto/8511/',
}

# ETAPA 2: Mapeamento de prefixos para LAUDOS
PREFIXOS_LAUDOS = {
    '0040': 'lab/Arquivos/Historico/0040/',
    '0085': 'lab/Arquivos/Historico/0085/',
    '0100': 'lab/Arquivos/Historico/0100/',
    '0101': 'lab/Arquivos/Historico/0101/',
    '0200': 'lab/Arquivos/Historico/0200/',
    '0031': 'lab/Arquivos/Historico/0031/',
    '0049': 'lab/Arquivos/Historico/0049/',
    '0102': 'lab/Arquivos/Historico/0102/',
    '0103': 'lab/Arquivos/Historico/0103/',
    '0300': 'lab/Arquivos/Historico/0300/',
    '8511': 'lab/Arquivos/Historico/8511/',
    '0032': 'lab/Arquivos/Historico/0032/',
}

def conectar_s3():
    """Conecta ao S3 com configurações otimizadas"""
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
        config=boto3.session.Config(
            max_pool_connections=50,  # Aumenta pool de conexões
            retries={'max_attempts': 2}  # Reduz tentativas de retry
        )
    )

def detectar_prefixo(nome_arquivo, prefixos_dict, default_prefix):
    """Detecta o prefixo da pasta baseado no nome do arquivo"""
    for codigo, caminho in prefixos_dict.items():
        if nome_arquivo.startswith(codigo):
            return caminho
    return default_prefix

def buscar_arquivo_s3(s3_client, nome_arquivo, extensao, prefixos_dict, default_prefix):
    """Busca arquivo no S3 pelo nome - OTIMIZADO (funciona para imagens e laudos)"""
    # Detectar pasta
    prefixo_busca = detectar_prefixo(nome_arquivo, prefixos_dict, default_prefix)

    # Nome completo do arquivo
    nome_completo = f"{nome_arquivo}.{extensao.lower()}"

    # OTIMIZAÇÃO 1: Tentar buscar diretamente o arquivo primeiro (mais rápido)
    caminho_direto = f"{prefixo_busca}{nome_completo}"

    try:
        # Tentar HEAD request primeiro (muito mais rápido que listar)
        s3_client.head_object(Bucket=BUCKET_NAME, Key=caminho_direto)
        return caminho_direto
    except:
        # Se não encontrar com o nome exato, tentar case-insensitive
        pass

    # OTIMIZAÇÃO 2: Se não encontrou direto, listar pasta (mas só até encontrar)
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=BUCKET_NAME,
            Prefix=prefixo_busca,
            PaginationConfig={'PageSize': 1000}  # Aumenta tamanho da página
        )

        for page in pages:
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                key = obj['Key']
                file_name = key.split('/')[-1]

                # Comparar nome (case insensitive)
                if file_name.lower() == nome_completo.lower():
                    return key

        return None
    except Exception as e:
        return None

def baixar_arquivo(s3_client, key, destino):
    """Baixa arquivo do S3"""
    try:
        s3_client.download_file(BUCKET_NAME, key, str(destino))
        return True
    except Exception as e:
        return False

def processar_imagem(linha, s3, total, contadores, lock, erros):
    """Processa uma imagem individualmente (usado em paralelo)"""
    nome_arquivo = linha['NomArquivo'].strip()
    extensao = linha['ExtArquivo'].strip()

    nome_completo = f"{nome_arquivo}.{extensao.lower()}"
    destino = DESTINO_IMAGENS / nome_completo

    # Verificar se já existe
    if destino.exists():
        with lock:
            contadores['ja_existe'] += 1
            contadores['processados'] += 1
            print(f"[IMAGEM {contadores['processados']}/{total}] {nome_completo:<40} JÁ EXISTE")
        return

    # Buscar no S3
    key = buscar_arquivo_s3(s3, nome_arquivo, extensao, PREFIXOS_IMAGENS, IMAGE_PREFIX)

    if not key:
        with lock:
            contadores['nao_encontrado'] += 1
            contadores['processados'] += 1
            erros.append(f"IMAGEM: {nome_completo} - Não encontrado no S3")
            print(f"[IMAGEM {contadores['processados']}/{total}] {nome_completo:<40} NÃO ENCONTRADO")
        return

    # Baixar
    if baixar_arquivo(s3, key, destino):
        tamanho_kb = destino.stat().st_size / 1024
        with lock:
            contadores['sucesso'] += 1
            contadores['processados'] += 1
            print(f"[IMAGEM {contadores['processados']}/{total}] {nome_completo:<40} OK ({tamanho_kb:.1f}KB)")
    else:
        with lock:
            contadores['falha'] += 1
            contadores['processados'] += 1
            erros.append(f"IMAGEM: {nome_completo} - Erro ao baixar")
            print(f"[IMAGEM {contadores['processados']}/{total}] {nome_completo:<40} ERRO AO BAIXAR")

def processar_laudo(linha, s3, total, contadores, lock, erros):
    """Processa um laudo individualmente (usado em paralelo)"""
    nome_arquivo = linha['CodRequisicao_extraido'].strip()

    # Se CodRequisicao_extraido está vazio, pular
    if not nome_arquivo:
        with lock:
            contadores['sem_laudo'] += 1
            contadores['processados'] += 1
            print(f"[LAUDO {contadores['processados']}/{total}] {'(vazio)':<40} PULADO - SEM LAUDO")
        return

    codigo_empresa = nome_arquivo[:4] if len(nome_arquivo) >= 4 else ""
    if codigo_empresa in PREFIXOS_LAUDOS:
        prefixo_laudo = PREFIXOS_LAUDOS[codigo_empresa]
    else:
        prefixo_laudo = 'lab/Arquivos/Historico/'

    key = None
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=BUCKET_NAME,
            Prefix=prefixo_laudo,
            PaginationConfig={'PageSize': 1000}
        )

        candidatos = []
        for page in pages:
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                file_name = obj['Key'].split('/')[-1]
                # Verifica se o nome do arquivo na nuvem começa com o código da requisição
                if file_name.lower().startswith(nome_arquivo.lower()):
                    candidatos.append(obj)
        
        # Se encontrou arquivos que começam com o código, escolhe o mais recente
        if candidatos:
            # Ordena os candidatos pela data de modificação (mais recente primeiro)
            candidatos.sort(key=lambda o: o['LastModified'], reverse=True)
            key = candidatos[0]['Key'] # Pega o mais recente

    except Exception as e:
        key = None
        print(f"[ERRO] Falha ao listar laudos para {nome_arquivo}: {e}")

    # Define o nome do arquivo de destino com base no que foi encontrado
    if key:
        nome_completo = key.split('/')[-1]
        destino = DESTINO_LAUDOS / nome_completo
        # Verifica se o arquivo mais recente já existe localmente
        if destino.exists():
            with lock:
                contadores['ja_existe'] += 1
                contadores['processados'] += 1
                print(f"[LAUDO {contadores['processados']}/{total}] {nome_completo:<40} JÁ EXISTE")
            return
    else:
        nome_completo = f"{nome_arquivo}.pdf" # Nome padrão para logs de erro

    if not key:
        with lock:
            contadores['nao_encontrado'] += 1
            contadores['processados'] += 1
            erros.append(f"LAUDO: {nome_completo} - Não encontrado no S3")
            print(f"[LAUDO {contadores['processados']}/{total}] {nome_completo:<40} NÃO ENCONTRADO")
        return

    # Baixar
    destino = DESTINO_LAUDOS / nome_completo
    if baixar_arquivo(s3, key, destino):
        tamanho_kb = destino.stat().st_size / 1024
        with lock:
            contadores['sucesso'] += 1
            contadores['processados'] += 1
            print(f"[LAUDO {contadores['processados']}/{total}] {nome_completo:<40} OK ({tamanho_kb:.1f}KB)")
    else:
        with lock:
            contadores['falha'] += 1
            contadores['processados'] += 1
            erros.append(f"LAUDO: {nome_completo} - Erro ao baixar")
            print(f"[LAUDO {contadores['processados']}/{total}] {nome_completo:<40} ERRO AO BAIXAR")

def processar_csv():
    """Processa o CSV e baixa IMAGENS e LAUDOS em 2 etapas"""
    csv_path = obter_caminho_processar_csv()

    print("="*80)
    print("DOWNLOAD AUTOMÁTICO - IMAGENS E LAUDOS DO S3")
    print("="*80)
    print(f"CSV: {csv_path}")
    print(f"Destino Imagens: {DESTINO_IMAGENS}")
    print(f"Destino Laudos: {DESTINO_LAUDOS}")
    print("="*80)

    # --- OTIMIZAÇÃO: Criar o cliente S3 uma única vez e reutilizá-lo ---
    s3_client = conectar_s3()

    # Criar pastas de destino
    DESTINO_IMAGENS.mkdir(exist_ok=True)
    DESTINO_LAUDOS.mkdir(exist_ok=True)

    # Ler CSV
    print(f"\n[INFO] Lendo CSV...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        linhas = list(reader)

    total = len(linhas)
    print(f"[INFO] Total de registros: {total}")
    print(f"[INFO] Usando 20 threads paralelas para download ultra-rápido")

    inicio_geral = datetime.now()
    erros = []

    # ============================================================
    # ETAPA 1: BAIXAR IMAGENS
    # ============================================================
    print("\n" + "="*80)
    print("ETAPA 1/2: BAIXANDO IMAGENS")
    print("="*80)

    contadores_imagens = {
        'sucesso': 0,
        'falha': 0,
        'nao_encontrado': 0,
        'ja_existe': 0,
        'processados': 0
    }
    lock = Lock()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for linha in linhas:
            future = executor.submit(processar_imagem, linha, s3_client, total, contadores_imagens, lock, erros)
            futures.append(future)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                with lock:
                    contadores_imagens['falha'] += 1
                    erros.append(f"IMAGEM - Erro inesperado: {str(e)}")

    tempo_imagens = (datetime.now() - inicio_geral).total_seconds()

    print("\n" + "="*80)
    print("RESUMO ETAPA 1 - IMAGENS")
    print("="*80)
    print(f"Sucesso:          {contadores_imagens['sucesso']}")
    print(f"Já existiam:      {contadores_imagens['ja_existe']}")
    print(f"Não encontrados:  {contadores_imagens['nao_encontrado']}")
    print(f"Falhas:           {contadores_imagens['falha']}")
    print(f"Tempo:            {tempo_imagens:.1f}s")
    print("="*80)

    # ============================================================
    # ETAPA 2: BAIXAR LAUDOS
    # ============================================================
    print("\n" + "="*80)
    print("ETAPA 2/2: BAIXANDO LAUDOS")
    print("="*80)

    contadores_laudos = {
        'sucesso': 0,
        'falha': 0,
        'nao_encontrado': 0,
        'ja_existe': 0,
        'processados': 0,
        'sem_laudo': 0
    }

    inicio_laudos = datetime.now()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for linha in linhas:
            future = executor.submit(processar_laudo, linha, s3_client, total, contadores_laudos, lock, erros)
            futures.append(future)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                with lock:
                    contadores_laudos['falha'] += 1
                    erros.append(f"LAUDO - Erro inesperado: {str(e)}")

    tempo_laudos = (datetime.now() - inicio_laudos).total_seconds()

    print("\n" + "="*80)
    print("RESUMO ETAPA 2 - LAUDOS")
    print("="*80)
    print(f"Sucesso:          {contadores_laudos['sucesso']}")
    print(f"Já existiam:      {contadores_laudos['ja_existe']}")
    print(f"Sem laudo:        {contadores_laudos['sem_laudo']}")
    print(f"Não encontrados:  {contadores_laudos['nao_encontrado']}")
    print(f"Falhas:           {contadores_laudos['falha']}")
    print(f"Tempo:            {tempo_laudos:.1f}s")
    print("="*80)

    # ============================================================
    # RESUMO FINAL
    # ============================================================
    tempo_total = (datetime.now() - inicio_geral).total_seconds()

    print("\n" + "="*80)
    print("RESUMO FINAL - IMAGENS + LAUDOS")
    print("="*80)
    print(f"Total registros:     {total}")
    print(f"")
    print(f"IMAGENS:")
    print(f"  ✓ Sucesso:         {contadores_imagens['sucesso']}")
    print(f"  ⊙ Já existiam:     {contadores_imagens['ja_existe']}")
    print(f"  ✗ Não encontrados: {contadores_imagens['nao_encontrado']}")
    print(f"  ✗ Falhas:          {contadores_imagens['falha']}")
    print(f"")
    print(f"LAUDOS:")
    print(f"  ✓ Sucesso:         {contadores_laudos['sucesso']}")
    print(f"  ⊙ Já existiam:     {contadores_laudos['ja_existe']}")
    print(f"  - Sem laudo:       {contadores_laudos['sem_laudo']}")
    print(f"  ✗ Não encontrados: {contadores_laudos['nao_encontrado']}")
    print(f"  ✗ Falhas:          {contadores_laudos['falha']}")
    print(f"")
    print(f"Tempo total:         {tempo_total:.1f}s")
    print(f"Velocidade média:    {(total * 2) / tempo_total:.1f} arquivos/segundo")
    print("="*80)

    # Salvar log de erros
    if erros:
        log_path = DESTINO_IMAGENS.parent / 'erros_download.txt'
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(f"LOG DE ERROS - {datetime.now()}\n")
            f.write("="*80 + "\n\n")
            for erro in erros:
                f.write(f"{erro}\n")
        print(f"\n[INFO] Log de erros salvo em: {log_path}")

    print(f"\n[INFO] Imagens salvas em: {DESTINO_IMAGENS}")
    print(f"[INFO] Laudos salvos em:  {DESTINO_LAUDOS}")

#######################################
##  Corrigir nomes dos arquivos baixados
#######################################

def renomear_arquivos_baixados():
    """Renomeia os arquivos baixados usando a API."""
    # --- Configurações da API ---
    url = "https://lab.aplis.inf.br/api/integracao.php"
    username = "api.lab"
    password = "nintendo64"
    headers = {
        "Content-Type": "application/json"
    }

    # Usar pasta temporária da execução atual
    folder_path = str(get_temp_dir())

    contador_requisicoes = 0
    pausa_a_cada = 25
    pausa_segundos = 20

    num_guia_counts = {}
    num_guia_laudo_set = set()
    arquivos_por_codigo = defaultdict(list)
    respostas_api = {}

    # Verifica se a pasta existe
    if not os.path.isdir(folder_path):
        print(f"Erro: O diretório '{folder_path}' não foi encontrado.")
        return

    print("\n" + "="*50)
    print("ETAPA 3: Renomeando arquivos baixados...")
    print("="*50)
    try:
        # Obtém a lista de arquivos com seus caminhos completos
        files_list = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
        
        # Ordena a lista de arquivos pelo tempo de modificação, do mais recente para o mais antigo
        files_list.sort(key=os.path.getmtime, reverse=True)
        
    except OSError as e:
        print(f"Erro ao acessar o diretório: {e}")
        files_list = []

    if not files_list:
        print("Nenhum arquivo encontrado no diretório.")
    
    # Primeiro, agrupa os arquivos por código de requisição
    for full_path in files_list:
        filename = os.path.basename(full_path)
        if len(filename) >= 13:
            codigo = filename[:13]
            arquivos_por_codigo[codigo].append(full_path)
        else:
            print(f"Aviso: O arquivo '{filename}' não possui 13 ou mais caracteres e será ignorado.")
    
    if arquivos_por_codigo:
        print("Iniciando as requisições...")
        print("---")
        
        for codRequisicao, arquivos in arquivos_por_codigo.items():
            print(f"Processando código de requisição: {codRequisicao} ({len(arquivos)} arquivo(s))")
            
            # Faz a requisição à API uma vez por código
            payload = {
                "ver": 1,
                "cmd": "requisicaoResultado",
                "dat": {
                    "codRequisicao": codRequisicao
                }
            }
            data = json.dumps(payload)
            
            try:
                response = requests.post(url, auth=(username, password), headers=headers, data=data)
                
                if response.status_code == 200:
                    try:
                        # Decodifica a resposta usando 'utf-8-sig' para remover o BOM (Byte Order Mark)
                        # antes de carregar o JSON. Isso é mais robusto que response.json().
                        resposta_json = json.loads(response.content.decode('utf-8-sig'))
                        respostas_api[codRequisicao] = resposta_json
                        
                        # Coleta todas as guias disponíveis em procedimentosCobrados
                        guias_disponiveis = []
                        if resposta_json.get("dat") and resposta_json["dat"].get("procedimentosCobrados"):
                            for procedimento in resposta_json["dat"]["procedimentosCobrados"]:
                                if procedimento.get("numGuia"):
                                    num_guia = procedimento["numGuia"]
                                    if num_guia not in guias_disponiveis:
                                        guias_disponiveis.append(num_guia)
                        
                        # Se não encontrou em procedimentosCobrados, tenta guiaPrincipal
                        if not guias_disponiveis and resposta_json.get("dat") and resposta_json["dat"].get("guiaPrincipal"):
                            if resposta_json["dat"]["guiaPrincipal"].get("numGuia"):
                                guias_disponiveis.append(resposta_json["dat"]["guiaPrincipal"]["numGuia"])

                        if guias_disponiveis:
                            print(f"  - Guias disponíveis: {guias_disponiveis}")
                            
                            # Se há 2+ guias, cria conjuntos iguais para ambas as guias (mesmo com poucos arquivos)
                            if len(guias_disponiveis) >= 2:
                                print(f"  → Detectados {len(arquivos)} arquivos com 2+ guias. Criando conjuntos iguais para ambas as guias...")
                                
                                primeira_guia = guias_disponiveis[0]
                                segunda_guia = guias_disponiveis[1]
                                
                                # Calcula quantos arquivos por guia (divide igualmente)
                                arquivos_por_guia = len(arquivos) // 2
                                arquivos_restantes = len(arquivos) % 2
                                
                                # Para 1-3 arquivos, garante que primeira guia tenha pelo menos 1
                                if len(arquivos) < 4:
                                    arquivos_primeira_guia_count = max(1, arquivos_por_guia + (1 if arquivos_restantes > 0 else 0))
                                    arquivos_segunda_guia_count = len(arquivos) - arquivos_primeira_guia_count
                                else:
                                    arquivos_primeira_guia_count = arquivos_por_guia + (1 if arquivos_restantes > 0 else 0)
                                    arquivos_segunda_guia_count = len(arquivos) - arquivos_primeira_guia_count
                                
                                print(f"  → Primeira guia: {arquivos_primeira_guia_count} arquivos, Segunda guia: {arquivos_segunda_guia_count} arquivos originais")
                                
                                # Separa os arquivos para cada guia
                                arquivos_primeira_guia = arquivos[:arquivos_primeira_guia_count]
                                arquivos_segunda_guia = arquivos[arquivos_primeira_guia_count:arquivos_primeira_guia_count + arquivos_segunda_guia_count]
                                
                                # Lista para armazenar caminhos dos arquivos da primeira guia (para copiar)
                                arquivos_processados_primeira = []
                                
                                # PROCESSA PRIMEIRA GUIA
                                print(f"  ▶ Processando {len(arquivos_primeira_guia)} arquivos para primeira guia ({primeira_guia}):")
                                for i, full_path in enumerate(arquivos_primeira_guia):
                                    filename = os.path.basename(full_path)
                                    nome_base, extensao = os.path.splitext(filename)
                                    
                                    # Define o sufixo
                                    if primeira_guia not in num_guia_laudo_set:
                                        sufixo = "_Laudo_do_profissional_de_saude"
                                        num_guia_laudo_set.add(primeira_guia)
                                    else:
                                        sufixo = "_GUIA"
                                    
                                    # Incrementa contador
                                    num_guia_counts[primeira_guia] = num_guia_counts.get(primeira_guia, 0) + 1
                                    count = num_guia_counts[primeira_guia]
                                    
                                    novo_nome = f"{primeira_guia}{sufixo}_{count}{extensao}"
                                    novo_caminho = os.path.join(folder_path, novo_nome)
                                    
                                    try:
                                        os.rename(full_path, novo_caminho)
                                        print(f"    → Renomeado: {filename} → {novo_nome}")
                                        arquivos_processados_primeira.append(novo_caminho)
                                    except OSError as e:
                                        print(f"    → Erro ao renomear {filename}: {e}")
                                
                                # PROCESSA SEGUNDA GUIA (renomeia arquivos originais)
                                print(f"  ▶ Processando {len(arquivos_segunda_guia)} arquivos para segunda guia ({segunda_guia}):")
                                arquivos_processados_segunda = []
                                
                                for i, full_path in enumerate(arquivos_segunda_guia):
                                    filename = os.path.basename(full_path)
                                    nome_base, extensao = os.path.splitext(filename)
                                    
                                    # Define o sufixo
                                    if segunda_guia not in num_guia_laudo_set:
                                        sufixo = "_Laudo_do_profissional_de_saude"
                                        num_guia_laudo_set.add(segunda_guia)
                                    else:
                                        sufixo = "_GUIA"
                                    
                                    # Incrementa contador
                                    num_guia_counts[segunda_guia] = num_guia_counts.get(segunda_guia, 0) + 1
                                    count = num_guia_counts[segunda_guia]
                                    
                                    novo_nome = f"{segunda_guia}{sufixo}_{count}{extensao}"
                                    novo_caminho = os.path.join(folder_path, novo_nome)
                                    
                                    try:
                                        os.rename(full_path, novo_caminho)
                                        print(f"    → Renomeado: {filename} → {novo_nome}")
                                        arquivos_processados_segunda.append(novo_caminho)
                                    except OSError as e:
                                        print(f"    → Erro ao renomear {filename}: {e}")
                                
                                # CRIA CÓPIAS PARA IGUALAR AS QUANTIDADES
                                total_primeira_guia = len(arquivos_processados_primeira)
                                total_segunda_guia = len(arquivos_processados_segunda)
                                diferenca = total_primeira_guia - total_segunda_guia
                                
                                if diferenca > 0:
                                    # Primeira guia tem mais arquivos - copia para segunda guia para igualar
                                    print(f"  ▶ Criando {diferenca} cópia(s) da primeira guia para segunda guia:")
                                    for i in range(diferenca):
                                        if i < len(arquivos_processados_primeira):
                                            arquivo_fonte = arquivos_processados_primeira[i]
                                            nome_fonte = os.path.basename(arquivo_fonte)
                                            _, extensao = os.path.splitext(nome_fonte)
                                            
                                            # Define sufixo para cópia
                                            sufixo = "_GUIA"
                                            
                                            # Incrementa contador segunda guia
                                            num_guia_counts[segunda_guia] = num_guia_counts.get(segunda_guia, 0) + 1
                                            count = num_guia_counts[segunda_guia]
                                            
                                            novo_nome_copia = f"{segunda_guia}{sufixo}_{count}{extensao}"
                                            novo_caminho_copia = os.path.join(folder_path, novo_nome_copia)
                                            
                                            try:
                                                shutil.copy2(arquivo_fonte, novo_caminho_copia)
                                                print(f"    → Copiado: {nome_fonte} → {novo_nome_copia}")
                                                total_segunda_guia += 1
                                            except (OSError, shutil.Error) as e:
                                                print(f"    → Erro ao copiar {nome_fonte}: {e}")
                                
                                elif diferenca < 0:
                                    # Segunda guia tem mais arquivos - copia para primeira guia para igualar
                                    print(f"  ▶ Criando {abs(diferenca)} cópia(s) da segunda guia para primeira guia:")
                                    for i in range(abs(diferenca)):
                                        if i < len(arquivos_processados_segunda):
                                            arquivo_fonte = arquivos_processados_segunda[i]
                                            nome_fonte = os.path.basename(arquivo_fonte)
                                            _, extensao = os.path.splitext(nome_fonte)
                                            
                                            # Define sufixo para cópia
                                            sufixo = "_GUIA"
                                            
                                            # Incrementa contador primeira guia
                                            num_guia_counts[primeira_guia] = num_guia_counts.get(primeira_guia, 0) + 1
                                            count = num_guia_counts[primeira_guia]
                                            
                                            novo_nome_copia = f"{primeira_guia}{sufixo}_{count}{extensao}"
                                            novo_caminho_copia = os.path.join(folder_path, novo_nome_copia)
                                            
                                            try:
                                                shutil.copy2(arquivo_fonte, novo_caminho_copia)
                                                print(f"    → Copiado: {nome_fonte} → {novo_nome_copia}")
                                                total_primeira_guia += 1
                                            except (OSError, shutil.Error) as e:
                                                print(f"    → Erro ao copiar {nome_fonte}: {e}")
                                
                                print(f"  ✓ Finalizado: Primeira guia ({primeira_guia}) = {total_primeira_guia} arquivos")
                                print(f"  ✓ Finalizado: Segunda guia ({segunda_guia}) = {total_segunda_guia} arquivos")
                            
                            # Lógica original apenas para quando há UMA guia
                            else:
                                print("  → Processamento padrão (apenas 1 guia disponível)")
                                for i, full_path in enumerate(arquivos):
                                    filename = os.path.basename(full_path)
                                    nome_base, extensao = os.path.splitext(filename)
                                    
                                    print(f"  Processando arquivo {i+1}: {filename}")
                                    
                                    # Lógica original
                                    if i < 3:
                                        num_guia_selecionado = guias_disponiveis[0]
                                    elif len(guias_disponiveis) >= 2:
                                        num_guia_selecionado = guias_disponiveis[1]
                                    else:
                                        num_guia_selecionado = guias_disponiveis[0]
                                    
                                    # Define sufixo
                                    if num_guia_selecionado not in num_guia_laudo_set:
                                        sufixo = "_Laudo_do_profissional_de_saude"
                                        num_guia_laudo_set.add(num_guia_selecionado)
                                    else:
                                        sufixo = "_GUIA"
                                    
                                    # Incrementa contador
                                    num_guia_counts[num_guia_selecionado] = num_guia_counts.get(num_guia_selecionado, 0) + 1
                                    count = num_guia_counts[num_guia_selecionado]
                                    
                                    novo_nome = f"{num_guia_selecionado}{sufixo}_{count}{extensao}"
                                    novo_caminho = os.path.join(folder_path, novo_nome)
                                    
                                    try:
                                        os.rename(full_path, novo_caminho)
                                        print(f"    → Renomeado: {filename} → {novo_nome}")
                                    except OSError as e:
                                        print(f"    → Erro ao renomear {filename}: {e}")
                        
                        else:
                            print(f"  - Aviso: Nenhum 'numGuia' válido foi encontrado na resposta para o código '{codRequisicao}'.")

                    except ValueError:
                        print(f"  - Erro: Resposta para {codRequisicao} não está em JSON.")
                else:
                    print(f"  - Erro na requisição (Status Code: {response.status_code}) para o código '{codRequisicao}'.")
                
            except requests.exceptions.RequestException as e:
                print(f"  - Erro na requisição para {codRequisicao}: {e}")
            
            contador_requisicoes += 1
            
            if contador_requisicoes % pausa_a_cada == 0:
                print("-" * 30)
                print(f"Número de requisições: {contador_requisicoes}. Pausando por {pausa_segundos} segundos...")
                print("-" * 30)
                time.sleep(pausa_segundos)
                
            print("-" * 50)
    else:
        print("Nenhum arquivo com nome de 13 ou mais caracteres foi encontrado para processar.")

    print("\n✅ Etapa 3 concluída - Renomeação finalizada!")

#######################################
## Unificar renomeação de arquivos baixados
#######################################

def extrair_primeiros_9_numeros(nome_arquivo):
    """Extrai os primeiros 9 números do nome do arquivo, ignorando se tiver mais de 9 antes do underline"""
    # Verificar se há underline no nome
    if '_' in nome_arquivo:
        parte_antes_underline = nome_arquivo.split('_')[0]
        # Se a parte antes do underline tem mais de 9 caracteres, ignorar o arquivo
        if len(parte_antes_underline) > 9:
            return None
        # Se tem exatamente 9 números, verificar se são todos números
        if len(parte_antes_underline) == 9 and parte_antes_underline.isdigit():
            return parte_antes_underline
        return None
    else:
        # Se não tem underline, verificar se começa com exatamente 9 números
        match = re.match(r'^(\d{9})', nome_arquivo)
        return match.group(1) if match else None

def converter_imagem_para_pdf(caminho_imagem, caminho_pdf_temp):
    """Converte imagem (JPG/PNG) para PDF"""
    try:
        img = Image.open(caminho_imagem)
        
        # Converter para RGB se necessário
        if img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Criar PDF temporário
        c = canvas.Canvas(caminho_pdf_temp, pagesize=A4)
        img_width, img_height = img.size
        
        # Calcular dimensões para caber na página A4
        page_width, page_height = A4
        aspect_ratio = img_width / img_height
        
        if aspect_ratio > 1:  # Imagem mais larga
            new_width = page_width - 40  # margem
            new_height = new_width / aspect_ratio
        else:  # Imagem mais alta
            new_height = page_height - 40  # margem
            new_width = new_height * aspect_ratio
        
        # Centralizar na página
        x = (page_width - new_width) / 2
        y = (page_height - new_height) / 2
        
        c.drawImage(caminho_imagem, x, y, width=new_width, height=new_height)
        c.save()
        return True
    except Exception as e:
        print(f"    ✗ Erro ao converter imagem {os.path.basename(caminho_imagem)}: {str(e)}")
        return False

def unificar_arquivos_por_grupo():
    """Função principal para unificar arquivos por grupo"""
    # Usar pasta temporária da execução atual
    pasta_origem = str(get_temp_dir())
    pasta_destino = pasta_origem  # Salvar na mesma pasta
    
    # Verificar se a pasta existe
    if not os.path.exists(pasta_origem):
        print(f"Erro: A pasta {pasta_origem} não existe!")
        return
    
    # Agrupar arquivos pelos primeiros 9 números
    grupos = defaultdict(list)
    extensoes_suportadas = {'.pdf', '.jpg', '.jpeg', '.png'}
    arquivos_ignorados = []
    
    print("Analisando arquivos na pasta...")
    for arquivo in os.listdir(pasta_origem):
        nome_arquivo = arquivo.lower()
        extensao = os.path.splitext(nome_arquivo)[1]
        
        if extensao in extensoes_suportadas:
            numero_grupo = extrair_primeiros_9_numeros(arquivo)
            if numero_grupo:
                caminho_completo = os.path.join(pasta_origem, arquivo)
                grupos[numero_grupo].append((caminho_completo, extensao, arquivo))  # incluir nome original
            else:
                # Verificar se foi ignorado por ter mais de 9 caracteres antes do underline
                if '_' in arquivo:
                    parte_antes_underline = arquivo.split('_')[0]
                    if len(parte_antes_underline) > 9:
                        arquivos_ignorados.append(arquivo)
    
    # Relatório inicial
    print(f"\n📊 RELATÓRIO INICIAL:")
    print(f"Encontrados {len(grupos)} grupos para processar:")
    for grupo, arquivos in grupos.items():
        print(f"  📁 Grupo {grupo}: {len(arquivos)} arquivos")
    
    if arquivos_ignorados:
        print(f"\n⚠️  {len(arquivos_ignorados)} arquivos ignorados (mais de 9 caracteres antes do '_'):")
        for arquivo in arquivos_ignorados[:5]:  # mostrar apenas os primeiros 5
            print(f"  - {arquivo}")
        if len(arquivos_ignorados) > 5:
            print(f"  ... e mais {len(arquivos_ignorados) - 5} arquivos")
    
    if not grupos:
        print("\n❌ Nenhum arquivo válido encontrado para processar!")
        return
    
    print(f"\n🔄 INICIANDO PROCESSAMENTO...")
    
    # Processar cada grupo
    for numero_grupo, arquivos in grupos.items():
        print(f"\n📋 Processando grupo {numero_grupo} ({len(arquivos)} arquivos)...")
        
        # Criar PDF unificado
        pdf_writer = PdfWriter()
        arquivos_temp = []
        arquivos_processados_com_sucesso = []
        
        try:
            # Ordenar arquivos por nome para ordem consistente
            arquivos_ordenados = sorted(arquivos, key=lambda x: x[2])  # ordenar pelo nome original
            
            for caminho_arquivo, extensao, nome_original in arquivos_ordenados:
                print(f"  📄 Processando: {nome_original}")
                
                if extensao == '.pdf':
                    # Adicionar PDF diretamente
                    try:
                        pdf_reader = PdfReader(caminho_arquivo)
                        for page_num, page in enumerate(pdf_reader.pages):
                            pdf_writer.add_page(page)
                        arquivos_processados_com_sucesso.append(caminho_arquivo)
                        print(f"    ✅ PDF adicionado com sucesso")
                    except Exception as e:
                        print(f"    ❌ Erro ao ler PDF: {str(e)}")
                        continue
                
                else:  # JPG ou PNG
                    # Converter imagem para PDF temporário
                    temp_pdf = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
                    temp_pdf.close()
                    arquivos_temp.append(temp_pdf.name)
                    
                    if converter_imagem_para_pdf(caminho_arquivo, temp_pdf.name):
                        try:
                            # Adicionar PDF temporário
                            pdf_reader = PdfReader(temp_pdf.name)
                            for page in pdf_reader.pages:
                                pdf_writer.add_page(page)
                            arquivos_processados_com_sucesso.append(caminho_arquivo)
                            print(f"    ✅ Imagem convertida e adicionada")
                        except Exception as e:
                            print(f"    ❌ Erro ao adicionar imagem convertida: {str(e)}")
                            continue
            
            # Verificar se pelo menos alguns arquivos foram processados
            if len(arquivos_processados_com_sucesso) == 0:
                print(f"  ❌ Nenhum arquivo foi processado com sucesso no grupo {numero_grupo}")
                print(f"  ⚠️  Arquivos originais NÃO serão excluídos")
                continue
            
            # Salvar PDF unificado
            nome_pdf_final = f"{numero_grupo}_GUIA_doc1.pdf"
            caminho_pdf_final = os.path.join(pasta_destino, nome_pdf_final)
            
            with open(caminho_pdf_final, 'wb') as arquivo_saida:
                pdf_writer.write(arquivo_saida)
            
            # Verificar se o PDF foi realmente criado
            if not os.path.exists(caminho_pdf_final):
                print(f"  ❌ Erro: PDF não foi criado - {nome_pdf_final}")
                print(f"  ⚠️  Arquivos originais NÃO serão excluídos")
                continue
                
            print(f"  ✅ PDF criado com sucesso: {nome_pdf_final}")
            
            # Excluir TODOS os arquivos originais do grupo (EXCETO o PDF unificado criado)
            print(f"  🗑️  Excluindo {len(arquivos)} arquivos originais do grupo...")
            arquivos_excluidos = 0
            arquivos_com_erro = []
            
            for caminho_arquivo, extensao, nome_original in arquivos:
                # NUNCA excluir o próprio PDF unificado que acabamos de criar
                if caminho_arquivo == caminho_pdf_final:
                    print(f"    ⚠️  Pulando PDF unificado: {nome_original}")
                    continue
                    
                try:
                    if os.path.exists(caminho_arquivo):
                        os.remove(caminho_arquivo)
                        arquivos_excluidos += 1
                        print(f"    ✅ Excluído: {nome_original}")
                    else:
                        print(f"    ⚠️  Arquivo não encontrado: {nome_original}")
                except Exception as e:
                    arquivos_com_erro.append(nome_original)
                    print(f"    ❌ Erro ao excluir {nome_original}: {str(e)}")
            
            print(f"  ✅ {arquivos_excluidos} arquivos originais excluídos")
            print(f"  🔒 PDF unificado preservado: {nome_pdf_final}")
            if arquivos_com_erro:
                print(f"  ⚠️  {len(arquivos_com_erro)} arquivos não puderam ser excluídos")
        
        except Exception as e:
            print(f"  ❌ Erro geral ao processar grupo {numero_grupo}: {str(e)}")
            print(f"  ⚠️  Arquivos originais NÃO foram excluídos devido ao erro")
        
        finally:
            # Limpar arquivos temporários
            for temp_file in arquivos_temp:
                try:
                    os.unlink(temp_file)
                except:
                    pass
    
    print(f"\n🎉 PROCESSO CONCLUÍDO!")
    print(f"📁 PDFs salvos em: {pasta_destino}")
    print(f"🔍 Procure por arquivos terminados em '_GUIA.pdf'")

def executar_processo_completo():
    """Executa todas as etapas em sequência após aprovação do usuário."""
    print("\n" + "="*80)
    print("INICIANDO PROCESSAMENTO COMPLETO")
    print("="*80)

    # Etapa 2: Download de imagens e laudos
    print("\n📥 ETAPA 2: Download de arquivos do S3...")
    processar_csv()

    # Etapa 3: Renomeação de arquivos
    print("\n🏷️  ETAPA 3: Renomeação de arquivos...")
    renomear_arquivos_baixados()

    # Etapa 4: Unificação em PDF
    print("\n📄 ETAPA 4: Unificação de arquivos em PDF...")
    unificar_arquivos_por_grupo()

    print("\n" + "="*80)
    print("✅ PROCESSO COMPLETO FINALIZADO COM SUCESSO!")
    print("="*80)

    # Sinaliza para o app.py que o download e processamento foram concluídos
    try:
        base_dir = Path(__file__).resolve().parent
        log_file = base_dir / 'download_completo.log'
        with open(log_file, 'w') as f:
            f.write(f"Concluído em: {datetime.now()}")
        print(f"✅ Sinal de conclusão para a interface web foi criado com sucesso.")
    except Exception as e:
        print(f"⚠️  Aviso: Não foi possível criar o arquivo de sinalização para a interface web: {e}")

#######################################
## Envio Orizon por API
#######################################

if __name__ == "__main__":
    if len(sys.argv) < 2:
        # Modo interativo (console)
        lote = int(input("Digite o número do Lote para filtrar: "))
        etapa_1_filtrar_requisicoes(lote, is_web_mode=False)
        # Se a etapa 1 não saiu, o usuário decidiu continuar
        # No modo console, se a etapa 1 não chamou sys.exit(), significa que o usuário confirmou.
        executar_processo_completo()
    else:
        # Modo via linha de comando (usado pelo app.py)
        comando = sys.argv[1]
        

        if comando == 'etapa1':
            lote = int(sys.argv[2])
            is_web = len(sys.argv) > 3 and sys.argv[3] == 'web'
            if is_web:
                print("Executando em MODO WEB. A interação será feita pela interface.")
            etapa_1_filtrar_requisicoes(lote, is_web_mode=is_web) # A função já contém sys.exit() para o modo web

        elif comando == 'continuar':
            print("\n" + "="*50)
            print("CONTINUANDO PROCESSAMENTO...")
            print("="*50)
            executar_processo_completo()
        
        else:
            print(f"Comando desconhecido: {comando}")
            sys.exit(1)