from flask import Flask, render_template, request, jsonify
import subprocess
import sys
import os
import pandas as pd
from werkzeug.utils import secure_filename

# Importa a função principal do script de envio
try:
    from enviar_anexos_producao import iniciar_processo_de_envio
except ImportError:
    print("AVISO: Não foi possível importar 'iniciar_processo_de_envio'. A finalização do processo não funcionará.")
    iniciar_processo_de_envio = None

app = Flask(__name__)

# Garante que o caminho para o script unificado_v1.py está correto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
SCRIPT_PATH = os.path.join(BASE_DIR, 'unificado_v1.py')
TEMP_PATH_FILE = os.path.join(BASE_DIR, 'temp_path.txt')

DECISION_FILE_PATH = os.path.join(BASE_DIR, 'decision.txt')
LOG_FILE_PATH = os.path.join(BASE_DIR, 'processamento.log')
DOWNLOAD_COMPLETE_LOG = os.path.join(BASE_DIR, 'download_completo.log')

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def obter_pasta_temporaria():
    """Lê o caminho da pasta temporária da execução atual."""
    if os.path.exists(TEMP_PATH_FILE):
        with open(TEMP_PATH_FILE, 'r') as f:
            pasta_temp = f.read().strip()
        return pasta_temp
    return None

def obter_caminhos_csv():
    """Lê o caminho da pasta temporária e retorna os caminhos dos arquivos CSV."""
    pasta_temp = obter_pasta_temporaria()
    if pasta_temp:
        incompletos_path = os.path.join(pasta_temp, "requisicaoimagem_filtrada_incompleta.csv")
        completos_path = os.path.join(pasta_temp, "requisicaoimagem_filtrada_final.csv")
        return incompletos_path, completos_path
    else:
        # Fallback para caminhos antigos (caso o arquivo não exista)
        return (None, None)


@app.route('/')
def index():
    """Renderiza a página inicial (index.html)."""
    return render_template('index.html')

@app.route('/api/limpar-estado', methods=['POST'])
def limpar_estado():
    """Limpa os arquivos de estado de execuções anteriores para um novo processamento."""
    try:
        incompletos_path, completos_path = obter_caminhos_csv()

        if os.path.exists(incompletos_path):
            os.remove(incompletos_path)
        if os.path.exists(completos_path):
            os.remove(completos_path)
        if os.path.exists(DECISION_FILE_PATH):
            os.remove(DECISION_FILE_PATH)
        if os.path.exists(LOG_FILE_PATH):
            os.remove(LOG_FILE_PATH)
        if os.path.exists(TEMP_PATH_FILE):
            os.remove(TEMP_PATH_FILE)
        if os.path.exists(DOWNLOAD_COMPLETE_LOG):
            os.remove(DOWNLOAD_COMPLETE_LOG)
        return jsonify({'status': 'sucesso', 'mensagem': 'Estado anterior limpo.'})
    except Exception as e:
        return jsonify({'status': 'erro', 'mensagem': f'Erro ao limpar estado: {e}'}), 500

# Rota da API para receber o número do lote e iniciar o processamento
@app.route('/api/processar-lote', methods=['POST'])
def processar_lote():
    """
    Recebe o número do lote do frontend e inicia o script de processamento.
    """
    data = request.get_json()
    numero_lote = data.get('numeroLote')

    if not numero_lote:
        return jsonify({'status': 'erro', 'mensagem': 'Número do lote não fornecido.'}), 400

    print(f"✅ Lote recebido: {numero_lote}. Iniciando processamento em segundo plano...")

    try:
        # Limpa o arquivo de log anterior
        if os.path.exists(LOG_FILE_PATH):
            os.remove(LOG_FILE_PATH)

        subprocess.Popen(
            [sys.executable, SCRIPT_PATH, 'etapa1', str(numero_lote), 'web']
        )

        # Responde imediatamente ao usuário enquanto o processo roda em background
        return jsonify({
            'status': 'sucesso',
            'mensagem': f'O processamento do lote {numero_lote} foi iniciado com sucesso!'
        })

    except FileNotFoundError:
        error_message = f"❌ Erro: O script 'unificado_v1.py' não foi encontrado no diretório."
        print(error_message)
        return jsonify({'status': 'erro', 'mensagem': error_message}), 500
    except Exception as e:
        print(f"❌ Erro ao iniciar o script: {e}")
        return jsonify({'status': 'erro', 'mensagem': f'Falha ao iniciar o processamento: {e}'}), 500

@app.route('/api/verificar-incompletos', methods=['GET'])
def verificar_incompletos():
    """Verifica se o arquivo de incompletos foi gerado e retorna seu conteúdo."""

    incompletos_path, completos_path = obter_caminhos_csv()

    # Se o usuário já decidiu continuar, o fluxo muda para verificar a conclusão do download.
    if os.path.exists(DECISION_FILE_PATH):
        if os.path.exists(DOWNLOAD_COMPLETE_LOG):
            return jsonify({'status': 'download_concluido'})
        else:
            # O download está em andamento, continue aguardando.
            return jsonify({'status': 'aguardando_download'})

    # Verifica se a etapa 1 foi concluída (arquivo de completos existe)
    if not os.path.exists(completos_path):
        return jsonify({'status': 'aguardando'}) # Etapa 1 ainda rodando

    # A etapa 1 foi concluída - verificar se há incompletos
    csv_existe = os.path.exists(incompletos_path)

    if csv_existe:
        try:
            # Lê a coluna CodRequisicao_extraido como string para preservar zeros à esquerda
            df_incompletos = pd.read_csv(incompletos_path, dtype={'CodRequisicao_extraido': str})

            if not df_incompletos.empty:
                # TEM INCOMPLETOS - retorna apenas a coluna CodRequisicao_extraido como texto
                # Extrai valores únicos da coluna CodRequisicao_extraido
                codigos_incompletos = df_incompletos['CodRequisicao_extraido'].unique()
                # Formata como texto, um por linha (já são strings, preservando zeros à esquerda)
                lista_codigos = '\n'.join(sorted(codigos_incompletos))
                df_completos = pd.read_csv(completos_path, dtype={'CodRequisicao_extraido': str})

                # Calcula estatísticas para ambos
                stats = {
                    'total_completos': len(df_completos),
                    'requisicoes_completas_unicas': df_completos['CodRequisicao_extraido'].nunique(),
                    'total_registros': len(df_incompletos),
                    'requisicoes_unicas': df_incompletos['CodRequisicao_extraido'].nunique(),
                    'tipos_counts': df_incompletos.groupby('CodRequisicao_extraido')['Tipo'].nunique().value_counts().to_dict()
                }

                return jsonify({
                    'status': 'encontrado',
                    'lista_codigos': lista_codigos,
                    'estatisticas': {
                        'total_completos': stats['total_completos'],
                        'requisicoes_completas_unicas': stats['requisicoes_completas_unicas'],
                        'total_registros': stats['total_registros'],
                        'requisicoes_unicas': stats['requisicoes_unicas']
                    }
                })
            else:
                # Arquivo existe mas está vazio - sem incompletos
                try:
                    df_completos = pd.read_csv(completos_path, dtype={'CodRequisicao_extraido': str})
                    return jsonify({
                        'status': 'sem_incompletos',
                        'estatisticas': {
                            'total_completos': len(df_completos),
                            'requisicoes_completas_unicas': df_completos['CodRequisicao_extraido'].nunique()
                        }
                    })
                except Exception as e:
                     return jsonify({'status': 'erro', 'mensagem': f"Erro ao ler arquivo de completos: {e}"})
        except Exception as e:
            return jsonify({'status': 'erro', 'mensagem': str(e)})
    else:
        # Arquivo de incompletos não existe - sem incompletos
        try:
            df_completos = pd.read_csv(completos_path, dtype={'CodRequisicao_extraido': str})
            return jsonify({
                'status': 'sem_incompletos',
                'estatisticas': {
                    'total_completos': len(df_completos),
                    'requisicoes_completas_unicas': df_completos['CodRequisicao_extraido'].nunique()
                }
            })
        except Exception as e:
            return jsonify({'status': 'erro', 'mensagem': str(e)})

@app.route('/api/decisao-continuar', methods=['POST'])
def decisao_continuar():
    """Recebe a decisão do usuário e a escreve em um arquivo."""
    data = request.get_json()
    decisao = data.get('decisao') # 'continuar' ou 'parar'

    if not decisao:
        return jsonify({'status': 'erro', 'mensagem': 'Decisão não fornecida.'}), 400

    try:
        with open(DECISION_FILE_PATH, 'w') as f:
            f.write(decisao)
        
        # Se a decisão for 'continuar', inicia a segunda parte do processo
        if decisao == 'continuar':
            print("✅ Decisão de continuar recebida. Preparando arquivos para a Etapa 2...")
            try:
                incompletos_path, completos_path = obter_caminhos_csv()

                # 1. Ler o arquivo de requisições completas
                df_completos = pd.read_csv(completos_path, dtype={'CodRequisicao_extraido': str})

                # 2. Verificar se o arquivo de incompletos existe e não está vazio
                df_incompletos = pd.DataFrame()
                if os.path.exists(incompletos_path):
                    try:
                        df_incompletos = pd.read_csv(incompletos_path, dtype={'CodRequisicao_extraido': str})
                    except pd.errors.EmptyDataError:
                        pass # Deixa o dataframe vazio se o arquivo estiver vazio

                # 3. Concatenar os dois dataframes
                df_processar = pd.concat([df_completos, df_incompletos], ignore_index=True)

                # 4. Salvar o resultado em um novo arquivo que será usado pela próxima etapa
                caminho_processar = os.path.join(os.path.dirname(completos_path), 'processar.csv')
                df_processar.to_csv(caminho_processar, index=False)
                print(f"   - Arquivo de processamento criado em: {caminho_processar} com {len(df_processar)} registros.")

                # 5. Chama o script para continuar, que agora usará 'processar.csv'
                print("   - Iniciando o restante do processo em segundo plano...")

                # Inicia o processo SEM redirecionamento - logs aparecem no terminal
                subprocess.Popen(
                    [sys.executable, SCRIPT_PATH, 'continuar']
                )

            except Exception as e:
                print(f"❌ Erro ao continuar o script: {e}")
                return jsonify({'status': 'erro', 'mensagem': f'Falha ao continuar o processamento: {e}'}), 500

        return jsonify({'status': 'sucesso', 'mensagem': f'Decisão "{decisao}" registrada.'})

    except Exception as e:
        return jsonify({'status': 'erro', 'mensagem': str(e)}), 500

@app.route('/api/processar-xml', methods=['POST'])
def processar_xml_endpoint():
    """
    Recebe o arquivo XML, salva, e dispara o processo de envio para a Orizon.
    """
    if not iniciar_processo_de_envio:
        return jsonify({'status': 'erro', 'mensagem': 'Erro de configuração no servidor: a função de envio não está disponível.'}), 500

    if 'xmlFile' not in request.files:
        return jsonify({'status': 'erro', 'mensagem': 'Nenhum arquivo XML foi enviado.'}), 400

    file = request.files['xmlFile']
    if file.filename == '':
        return jsonify({'status': 'erro', 'mensagem': 'Nome de arquivo inválido.'}), 400

    if file:
        filename = secure_filename(file.filename)
        caminho_xml_salvo = os.path.join(UPLOAD_FOLDER, filename)
        file.save(caminho_xml_salvo)

        print(f"✅ Arquivo XML '{filename}' salvo em '{caminho_xml_salvo}'.")
        print("🚀 Iniciando o processo de envio de anexos para a Orizon...")

        try:
            # Obter a pasta temporária da execução atual
            pasta_temp_atual = obter_pasta_temporaria()
            if not pasta_temp_atual:
                return jsonify({'status': 'erro', 'mensagem': 'Pasta temporária não encontrada. Execute o processamento primeiro.'}), 400
            
            # Chama a função do outro script com os caminhos corretos
            resultado = iniciar_processo_de_envio(
                caminho_xml=caminho_xml_salvo,
                pasta_pdfs=pasta_temp_atual
            )
            
            # Importa a função de limpeza e executa
            try:
                from unificado_v1 import limpar_pasta_temporaria
                print("🗑️  Limpando arquivos temporários...")
                limpar_pasta_temporaria()
                print("✅ Arquivos temporários removidos com sucesso.")
            except Exception as e_cleanup:
                print(f"⚠️  Aviso: Não foi possível limpar arquivos temporários: {e_cleanup}")
            
            if resultado.get('sucesso'):
                return jsonify({'status': 'sucesso', 'mensagem': resultado.get('mensagem', 'Processo concluído.')})
            else:
                return jsonify({'status': 'erro', 'mensagem': resultado.get('mensagem', 'Ocorreu um erro durante o envio.')})

        except Exception as e:
            print(f"❌ Ocorreu um erro inesperado ao chamar 'iniciar_processo_de_envio': {e}")
            return jsonify({'status': 'erro', 'mensagem': f'Erro inesperado no servidor: {e}'}), 500

    return jsonify({'status': 'erro', 'mensagem': 'Falha no upload do arquivo.'}), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)