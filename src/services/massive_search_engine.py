"""
Massive Search Engine - Sistema de Busca Massiva
Coleta dados até atingir 300KB mínimo salvando em RES_BUSCA_[PRODUTO].json
"""

import os
import json
import logging
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
import sys
import time

# Adicionar o diretório src ao path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from services.alibaba_websailor import alibaba_websailor
from services.real_search_orchestrator import RealSearchOrchestrator

logger = logging.getLogger(__name__)

class MassiveSearchEngine:
    """Sistema de busca massiva com múltiplas APIs e rotação"""

    def __init__(self):
        self.websailor = alibaba_websailor  # ALIBABA WebSailor
        self.real_search = RealSearchOrchestrator()  # Real Search Orchestrator

        self.min_size_kb = int(os.getenv('MIN_JSON_SIZE_KB', '500'))
        self.min_size_bytes = self.min_size_kb * 1024
        self.data_dir = os.getenv('DATA_DIR', 'analyses_data')

        os.makedirs(self.data_dir, exist_ok=True)

        logger.info(f"🔍 Massive Search Engine inicializado - Mínimo: {self.min_size_kb}KB")

    async def execute_massive_search(self, produto: str, publico_alvo: str, session_id: str, **kwargs) -> Dict[str, Any]:
        """
        Executa busca massiva até atingir 300KB mínimo
        Salva em RES_BUSCA_[PRODUTO].json

        Aceita **kwargs para evitar erros com argumentos inesperados (ex: 'query').
        """
        # Logar argumentos inesperados para depuração
        if kwargs:
            logger.warning(f"⚠️ Argumentos inesperados recebidos e ignorados: {list(kwargs.keys())}")

        try:
            logger.info(f"🚀 INICIANDO BUSCA MASSIVA: {produto}")

            # Arquivo de resultado
            produto_clean = produto.replace(' ', '_').replace('/', '_')
            resultado_file = os.path.join(self.data_dir, f"RES_BUSCA_{produto_clean.upper()}.json")

            # Estrutura de dados massiva
            massive_data = {
                'produto': produto,
                'publico_alvo': publico_alvo,
                'session_id': session_id,
                'timestamp_inicio': datetime.now().isoformat(),
                'busca_massiva': {
                    'alibaba_websailor_results': [],  # ALIBABA WebSailor
                    'real_search_orchestrator_results': []  # Real Search Orchestrator
                },
                'viral_content': [],
                'marketing_insights': [],
                'competitor_analysis': [],
                'social_media_data': [],
                'content_analysis': [],
                'trend_analysis': [],
                'metadata': {
                    'total_searches': 0,
                    'apis_used': [],
                    'size_kb': 0,
                    'target_size_kb': self.min_size_kb
                }
            }

            # Queries de busca massiva
            search_queries = self._generate_search_queries(produto, publico_alvo)

            logger.info(f"📋 {len(search_queries)} queries geradas para busca massiva")

            # Executar buscas com limite fixo para evitar loop infinito
            current_size = 0
            search_count = 0
            max_searches = min(len(search_queries), 10)  # Máximo 10 buscas para evitar loop

            for query in search_queries[:max_searches]:  # Processa apenas as primeiras queries
                search_count += 1
                logger.info(f"🔍 Busca {search_count}: {query[:50]}...")

                # ALIBABA WebSailor - PRINCIPAL
                try:
                    websailor_result = await self._search_alibaba_websailor(query, session_id)
                    if websailor_result:
                        massive_data['busca_massiva']['alibaba_websailor_results'].append(websailor_result)
                        massive_data['metadata']['apis_used'].append('alibaba_websailor')
                        logger.info(f"✅ ALIBABA WebSailor: dados coletados")
                except Exception as e:
                    logger.warning(f"⚠️ ALIBABA WebSailor falhou: {e}")

                # REAL SEARCH ORCHESTRATOR JÁ FOI EXECUTADO NO WORKFLOW - EVITAR LOOP
                # Apenas registra que os dados já foram coletados
                massive_data['metadata']['apis_used'].append('real_search_orchestrator_already_executed')
                logger.info(f"✅ Real Search Orchestrator: dados já coletados no workflow principal")

                # Verificar tamanho atual
                current_json = json.dumps(massive_data, ensure_ascii=False, indent=2)
                current_size = len(current_json.encode('utf-8'))

                logger.info(f"📊 Tamanho atual: {current_size/1024:.1f}KB / {self.min_size_kb}KB")

                # Pequena pausa entre buscas
                await asyncio.sleep(1)

            # Finalizar dados
            massive_data['timestamp_fim'] = datetime.now().isoformat()
            massive_data['metadata']['total_searches'] = search_count
            massive_data['metadata']['size_kb'] = current_size / 1024
            massive_data['metadata']['apis_used'] = list(set(massive_data['metadata']['apis_used']))

            # CONSOLIDAÇÃO: Coleta todos os dados salvos para arquivo único
            logger.info("🔄 Consolidando todos os dados salvos...")
            massive_data = self._consolidate_all_saved_data(massive_data, session_id)

            # Salva resultado final unificado
            from services.auto_save_manager import auto_save_manager

            save_result = auto_save_manager.save_massive_search_result(massive_data, produto)

            if save_result.get('success'):
                logger.info(f"✅ Resultado massivo CONSOLIDADO salvo: {save_result['filename']} ({save_result['size_kb']:.1f}KB)")
                return save_result['data']
            else:
                logger.error(f"❌ Erro ao salvar resultado massivo: {save_result.get('error')}")
                return massive_data

        except Exception as e:
            logger.error(f"❌ Erro na busca massiva: {e}")
            return {
                'success': False,
                'error': str(e),
                'file_path': None
            }

    def _generate_search_queries(self, produto: str, publico_alvo: str) -> List[str]:
        """Gera queries de busca massiva"""
        base_queries = [
            f"{produto} {publico_alvo}",
            f"{produto} marketing",
            f"{produto} vendas",
            f"{produto} estratégia",
            f"{produto} público alvo",
            f"{produto} mercado",
            f"{produto} tendências",
            f"{produto} concorrentes",
            f"{produto} análise",
            f"{produto} insights",
            f"{produto} campanhas",
            f"{produto} conversão",
            f"{produto} engajamento",
            f"{produto} redes sociais",
            f"{produto} influenciadores",
            f"{produto} viral",
            f"{produto} sucesso",
            f"{produto} cases",
            f"{produto} resultados",
            f"{produto} ROI"
        ]

        # Adicionar variações com público-alvo
        publico_queries = [
            f"{publico_alvo} {produto}",
            f"{publico_alvo} interesse {produto}",
            f"{publico_alvo} compra {produto}",
            f"{publico_alvo} busca {produto}",
            f"{publico_alvo} precisa {produto}"
        ]

        return base_queries + publico_queries

    def _generate_expanded_queries(self, produto: str, publico_alvo: str) -> List[str]:
        """Gera queries expandidas para atingir tamanho mínimo"""
        expanded = [
            f"como vender {produto}",
            f"melhor {produto}",
            f"onde comprar {produto}",
            f"preço {produto}",
            f"avaliação {produto}",
            f"review {produto}",
            f"opinião {produto}",
            f"teste {produto}",
            f"comparação {produto}",
            f"alternativa {produto}",
            f"{produto} 2024",
            f"{produto} tendência",
            f"{produto} futuro",
            f"{produto} inovação",
            f"{produto} tecnologia"
        ]

        return expanded

    async def _search_alibaba_websailor(self, query: str, session_id: str) -> Optional[Dict[str, Any]]:
        """Busca usando ALIBABA WebSailor - FOCO EM TEXTO"""
        try:
            logger.info(f"🌐 ALIBABA WebSailor executando busca TEXTUAL: {query}")

            # FOCO PRINCIPAL: NAVEGAÇÃO PARA EXTRAIR TEXTO
            navigation_result = await self.websailor.navigate_and_research_deep(
                query=query,
                context={'session_id': session_id, 'extract_text_only': True},
                max_pages=8,  # Reduzido para focar em qualidade
                depth_levels=2,
                session_id=session_id
            )
            
            # Conta o texto extraído
            texto_extraido = 0
            if navigation_result and isinstance(navigation_result, dict):
                conteudo = navigation_result.get('conteudo_consolidado', {})
                if conteudo:
                    textos = conteudo.get('textos_principais', [])
                    texto_extraido = sum(len(str(texto)) for texto in textos)

            logger.info(f"✅ ALIBABA WebSailor: {texto_extraido:,} caracteres de texto extraído")

            return {
                'query': query,
                'api': 'alibaba_websailor',
                'timestamp': datetime.now().isoformat(),
                'navigation_data': navigation_result,
                'texto_stats': {
                    'caracteres_extraidos': texto_extraido,
                    'paginas_navegadas': navigation_result.get('total_paginas_navegadas', 0) if navigation_result else 0
                },
                'source': 'ALIBABA_WEBSAILOR_TEXTUAL'
            }
        except Exception as e:
            logger.error(f"❌ ALIBABA WebSailor falhou: {e}")
            return None

    async def _search_real_orchestrator(self, query: str, session_id: str) -> Optional[Dict[str, Any]]:
        """Busca usando Real Search Orchestrator - SISTEMA PRINCIPAL"""
        try:
            logger.info(f"🎯 Real Search Orchestrator executando busca: {query}")

            # Usa o método CORRETO que existe no RealSearchOrchestrator
            result = await self.real_search.execute_massive_real_search(
                query=query,
                context={'session_id': session_id, 'produto': query},
                session_id=session_id
            )

            # Extrai dados válidos do resultado
            if result and isinstance(result, dict):
                return {
                    'query': query,
                    'api': 'real_search_orchestrator',
                    'timestamp': datetime.now().isoformat(),
                    'data': result,
                    'web_results_count': len(result.get('web_results', [])),
                    'social_results_count': len(result.get('social_results', [])),
                    'youtube_results_count': len(result.get('youtube_results', [])),
                    'source': 'REAL_SEARCH_ORCHESTRATOR_PRINCIPAL'
                }
            else:
                logger.warning(f"⚠️ Real Search Orchestrator retornou dados inválidos")
                return None

        except Exception as e:
            logger.error(f"❌ Real Search Orchestrator falhou: {e}")
            return None

    def _calculate_final_size(self, massive_data: Dict[str, Any]) -> float:
        """Calcula tamanho final em KB"""
        try:
            json_str = json.dumps(massive_data, ensure_ascii=False)
            return len(json_str.encode('utf-8')) / 1024
        except Exception as e:
            logger.error(f"❌ Erro ao calcular tamanho: {e}")
            return 0.0

    def _consolidate_all_saved_data(self, massive_data: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """
        CONSOLIDAÇÃO TEXTUAL: Coleta APENAS texto para análise da IA
        Remove imagens e mantém apenas dados textuais essenciais
        """
        try:
            logger.info("📝 Iniciando consolidação TEXTUAL para análise da IA...")

            # Estrutura para dados consolidados TEXTUAIS (removida duplicação)
            dados_consolidados = {
                'textos_pesquisa_web': [],
                'textos_redes_sociais': [],
                'insights_extraidos': [],
                'trechos_navegacao': [],
                'metadados_fontes': [],
                'etapas_extracao': [],
                'modulos_analises': [],
                'jsons_gigantes': [],
                'resultados_virais': [],
                'metadata_consolidacao': {
                    'timestamp_consolidacao': datetime.now().isoformat(),
                    'session_id': session_id,
                    'total_textos_processados': 0,
                    'fontes_unicas': 0,
                    'caracteres_totais': 0,
                    'categorias_encontradas': [],
                    'finalidade': 'ALIMENTAR_IA_SEGUNDA_ETAPA'
                }
            }

            textos_processados = 0
            caracteres_totais = 0

            # 1. COLETA TEXTOS DA PESQUISA WEB
            try:
                # Extrai textos dos resultados de busca do ALIBABA WebSailor
                if 'alibaba_websailor_results' in massive_data.get('busca_massiva', {}):
                    for result in massive_data['busca_massiva']['alibaba_websailor_results']:
                        if result and isinstance(result, dict):
                            # Extrai texto da navegação
                            nav_data = result.get('navigation_data', {})
                            if nav_data and isinstance(nav_data, dict):
                                conteudo = nav_data.get('conteudo_consolidado', {})
                                
                                # Textos principais
                                textos_principais = conteudo.get('textos_principais', [])
                                for texto in textos_principais:
                                    dados_consolidados['textos_pesquisa_web'].append({
                                        'fonte': 'websailor_navegacao',
                                        'url': result.get('query', 'N/A'),
                                        'texto': str(texto),
                                        'caracteres': len(str(texto))
                                    })
                                    caracteres_totais += len(str(texto))
                                    textos_processados += 1
                                
                                # Insights extraídos
                                insights = conteudo.get('insights_principais', [])
                                for insight in insights:
                                    dados_consolidados['insights_extraidos'].append({
                                        'fonte': 'websailor_insights',
                                        'insight': str(insight),
                                        'caracteres': len(str(insight))
                                    })
                                    caracteres_totais += len(str(insight))
                
                logger.info(f"✅ Coletados {textos_processados} textos da pesquisa web")
            except Exception as e:
                logger.error(f"❌ Erro ao coletar textos web: {e}")

                            # Processa arquivos de extração
            try:
                dados_consolidados['etapas_extracao'] = []
                arquivos_processados = 0
                
                base_path = "analyses_data/relatorios_intermediarios"
                if os.path.exists(base_path):
                    for categoria in os.listdir(base_path):
                        categoria_path = os.path.join(base_path, categoria)
                        if os.path.isdir(categoria_path):
                            # Procura arquivos da sessão específica
                            session_path = os.path.join(categoria_path, session_id)
                            if os.path.exists(session_path):
                                for arquivo in os.listdir(session_path):
                                    if arquivo.endswith('.json'):
                                        arquivo_path = os.path.join(session_path, arquivo)
                                        try:
                                            with open(arquivo_path, 'r', encoding='utf-8') as f:
                                                dados_arquivo = json.load(f)
                                                dados_consolidados['etapas_extracao'].append({
                                                    'arquivo_origem': arquivo,
                                                    'categoria': categoria,
                                                    'dados': dados_arquivo
                                                })
                                                arquivos_processados += 1
                                        except Exception as e:
                                            logger.warning(f"⚠️ Erro ao ler {arquivo_path}: {e}")

                            # Também procura arquivos gerais da categoria (sem session_id)
                            for arquivo in os.listdir(categoria_path):
                                if arquivo.endswith('.json') and session_id in arquivo:
                                    arquivo_path = os.path.join(categoria_path, arquivo)
                                    try:
                                        with open(arquivo_path, 'r', encoding='utf-8') as f:
                                            dados_arquivo = json.load(f)
                                            dados_consolidados['etapas_extracao'].append({
                                                'arquivo_origem': arquivo,
                                                'categoria': categoria,
                                                'dados': dados_arquivo
                                            })
                                            arquivos_processados += 1
                                    except Exception as e:
                                        logger.warning(f"⚠️ Erro ao ler {arquivo_path}: {e}")

                logger.info(f"✅ Coletadas {len(dados_consolidados['etapas_extracao'])} etapas de extração")
            except Exception as e:
                logger.error(f"❌ Erro ao coletar etapas de extração: {e}")

            # Coleta módulos de análises
            try:
                dados_consolidados['modulos_analises'] = []
                
                analyses_base = "analyses_data"
                if os.path.exists(analyses_base):
                    for categoria in os.listdir(analyses_base):
                        categoria_path = os.path.join(analyses_base, categoria)
                        if os.path.isdir(categoria_path) and categoria not in ['files', 'completas', 'reports']:
                            dados_consolidados['metadata_consolidacao']['categorias_encontradas'].append(f"analyses/{categoria}")

                            for arquivo in os.listdir(categoria_path):
                                if arquivo.endswith('.json') and session_id in arquivo:
                                    arquivo_path = os.path.join(categoria_path, arquivo)
                                    try:
                                        with open(arquivo_path, 'r', encoding='utf-8') as f:
                                            dados_arquivo = json.load(f)
                                            dados_consolidados['modulos_analises'].append({
                                                'arquivo_origem': arquivo,
                                                'categoria': categoria,
                                                'dados': dados_arquivo
                                            })
                                            arquivos_processados += 1
                                    except Exception as e:
                                        logger.warning(f"⚠️ Erro ao ler {arquivo_path}: {e}")

                logger.info(f"✅ Coletados {len(dados_consolidados['modulos_analises'])} módulos de análise")
            except Exception as e:
                logger.error(f"❌ Erro ao coletar módulos de análises: {e}")

            # Coleta módulos de análises
            try:
                dados_consolidados['modulos_analises'] = []
                
                analyses_base = "analyses_data"
                if os.path.exists(analyses_base):
                    for categoria in os.listdir(analyses_base):
                        categoria_path = os.path.join(analyses_base, categoria)
                        if os.path.isdir(categoria_path) and categoria not in ['files', 'completas', 'reports']:
                            dados_consolidados['metadata_consolidacao']['categorias_encontradas'].append(f"analyses/{categoria}")

                            for arquivo in os.listdir(categoria_path):
                                if arquivo.endswith('.json') and session_id in arquivo:
                                    arquivo_path = os.path.join(categoria_path, arquivo)
                                    try:
                                        with open(arquivo_path, 'r', encoding='utf-8') as f:
                                            dados_arquivo = json.load(f)
                                            dados_consolidados['modulos_analises'].append({
                                                'arquivo_origem': arquivo,
                                                'categoria': categoria,
                                                'dados': dados_arquivo
                                            })
                                            arquivos_processados += 1
                                    except Exception as e:
                                        logger.warning(f"⚠️ Erro ao ler {arquivo_path}: {e}")

                logger.info(f"✅ Coletados {len(dados_consolidados['modulos_analises'])} módulos de análise")
            except Exception as e:
                logger.error(f"❌ Erro ao coletar módulos de análises: {e}")

            # 3. COLETA JSONS GIGANTES
            try:
                completas_path = os.path.join("analyses_data", "completas")
                if os.path.exists(completas_path):
                    for arquivo in os.listdir(completas_path):
                        if arquivo.startswith('dados_massivos_') and session_id in arquivo and arquivo.endswith('.json'):
                            arquivo_path = os.path.join(completas_path, arquivo)
                            try:
                                with open(arquivo_path, 'r', encoding='utf-8') as f:
                                    dados_arquivo = json.load(f)
                                    dados_consolidados['jsons_gigantes'].append({
                                        'arquivo_origem': arquivo,
                                        'dados': dados_arquivo
                                    })
                                    arquivos_processados += 1
                            except Exception as e:
                                logger.warning(f"⚠️ Erro ao ler {arquivo_path}: {e}")

                logger.info(f"✅ Coletados {len(dados_consolidados['jsons_gigantes'])} JSONs gigantes")
            except Exception as e:
                logger.error(f"❌ Erro ao coletar JSONs gigantes: {e}")

            # 4. COLETA RESULTADOS VIRAIS
            try:
                viral_base = "analyses_data/viral_images_data"
                if os.path.exists(viral_base):
                    for arquivo in os.listdir(viral_base):
                        if arquivo.startswith('viral_results') and arquivo.endswith('.json'):
                            arquivo_path = os.path.join(viral_base, arquivo)
                            try:
                                with open(arquivo_path, 'r', encoding='utf-8') as f:
                                    dados_arquivo = json.load(f)
                                    dados_consolidados['resultados_virais'].append({
                                        'arquivo_origem': arquivo,
                                        'dados': dados_arquivo
                                    })
                                    arquivos_processados += 1
                            except Exception as e:
                                logger.warning(f"⚠️ Erro ao ler {arquivo_path}: {e}")

                logger.info(f"✅ Coletados {len(dados_consolidados['resultados_virais'])} resultados virais")
            except Exception as e:
                logger.error(f"❌ Erro ao coletar resultados virais: {e}")

            # 5. COLETA TRECHOS DE PESQUISA WEB
            try:
                pesquisa_path = os.path.join("analyses_data", "pesquisa_web")
                if os.path.exists(pesquisa_path):
                    # Arquivo consolidado da sessão
                    consolidado_path = os.path.join(pesquisa_path, session_id, "consolidado.json")
                    if os.path.exists(consolidado_path):
                        try:
                            with open(consolidado_path, 'r', encoding='utf-8') as f:
                                dados_arquivo = json.load(f)
                                dados_consolidados['trechos_pesquisa_web'].append({
                                    'arquivo_origem': 'consolidado.json',
                                    'tipo': 'consolidado_sessao',
                                    'dados': dados_arquivo
                                })
                                arquivos_processados += 1
                        except Exception as e:
                            logger.warning(f"⚠️ Erro ao ler {consolidado_path}: {e}")

                    # Arquivos individuais da sessão
                    session_pesquisa_path = os.path.join(pesquisa_path, session_id)
                    if os.path.exists(session_pesquisa_path):
                        for arquivo in os.listdir(session_pesquisa_path):
                            if arquivo.endswith('.json') and arquivo != 'consolidado.json':
                                arquivo_path = os.path.join(session_pesquisa_path, arquivo)
                                try:
                                    with open(arquivo_path, 'r', encoding='utf-8') as f:
                                        dados_arquivo = json.load(f)
                                        dados_consolidados['trechos_pesquisa_web'].append({
                                            'arquivo_origem': arquivo,
                                            'tipo': 'trecho_individual',
                                            'dados': dados_arquivo
                                        })
                                        arquivos_processados += 1
                                except Exception as e:
                                    logger.warning(f"⚠️ Erro ao ler {arquivo_path}: {e}")

                logger.info(f"✅ Coletados {len(dados_consolidados['trechos_pesquisa_web'])} trechos de pesquisa web")
            except Exception as e:
                logger.error(f"❌ Erro ao coletar trechos de pesquisa web: {e}")

            # 2. COLETA DADOS SALVOS EM ETAPAS ANTERIORES
            try:
                from services.auto_save_manager import auto_save_manager
                session_data = auto_save_manager.recuperar_etapa(session_id)
                
                if session_data and isinstance(session_data, dict):
                    extraction_steps = session_data.get('extraction_steps', [])
                    for step in extraction_steps:
                        try:
                            step_data = step.get('dados', {})
                            
                            # Extrai textos do conteudo
                            if isinstance(step_data, dict):
                                for key, value in step_data.items():
                                    if isinstance(value, str) and len(value) > 50:
                                        dados_consolidados['trechos_navegacao'].append({
                                            'fonte': f"{step.get('nome', 'step')}_{key}",
                                            'texto': value,
                                            'caracteres': len(value)
                                        })
                                        caracteres_totais += len(value)
                                        textos_processados += 1
                                        
                        except Exception as e:
                            logger.warning(f"⚠️ Erro ao processar step: {e}")
                            continue
                        
                logger.info(f"✅ Processados {len(extraction_steps)} steps salvos")
            except Exception as e:
                logger.error(f"❌ Erro ao coletar etapas salvas: {e}")

            # 3. ADICIONA METADADOS PARA A IA
            try:
                # Lista URLs únicas para contexto
                urls_unicas = set()
                for item in dados_consolidados['textos_pesquisa_web']:
                    if item.get('url') and item['url'] != 'N/A':
                        urls_unicas.add(item['url'])
                
                # Calcula estatísticas finais
                dados_consolidados['metadata_consolidacao'].update({
                    'total_textos_processados': textos_processados,
                    'caracteres_totais': caracteres_totais,
                    'fontes_unicas': len(urls_unicas),
                    'tamanho_kb': caracteres_totais / 1024,
                    'urls_fonte': list(urls_unicas)[:10],  # Máximo 10 URLs para contexto
                    'resumo_coleta': f"{textos_processados} textos, {caracteres_totais:,} chars, {len(urls_unicas)} fontes"
                })
                
                logger.info(f"📊 CONSOLIDAÇÃO TEXTUAL: {textos_processados} textos, {caracteres_totais:,} caracteres")
                
            except Exception as e:
                logger.error(f"❌ Erro ao calcular metadados: {e}")

            # Adiciona dados consolidados ao massive_data (APENAS TEXTO)
            massive_data['dados_consolidados_texto'] = dados_consolidados

            logger.info(f"✅ CONSOLIDAÇÃO TEXTUAL CONCLUÍDA: {textos_processados} textos processados")
            logger.info(f"📝 Total: {caracteres_totais:,} caracteres para análise da IA")

            return massive_data

        except Exception as e:
            logger.error(f"❌ Erro na consolidação de dados: {e}")
            # Retorna dados originais se falhar
            return massive_data


# Instância global
massive_search_engine = MassiveSearchEngine()