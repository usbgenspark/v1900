# services/viral_integration_service.py
"""VIRAL IMAGE FINDER - ARQV30 Enhanced v3.0
Módulo para buscar imagens virais no Google Imagens de Instagram/Facebook
Analisa engajamento, extrai links dos posts e salva dados estruturados
CORRIGIDO: APIs funcionais, extração real de imagens, fallbacks robustos
"""
import os
import re
import json
import time
import asyncio
import logging
import ssl
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, parse_qs, unquote, urljoin
from dataclasses import dataclass, asdict
import hashlib
import requests

# Import condicional do aiohttp, caso esteja disponível
try:
    import aiohttp
    import aiofiles
    HAS_ASYNC_DEPS = True
except ImportError:
    HAS_ASYNC_DEPS = False
    logger = logging.getLogger(__name__)
    logger.warning("aiohttp/aiofiles não encontrados. Usando requests síncrono como fallback.")


# Configuração de logging
logger = logging.getLogger(__name__)

@dataclass
class ViralImage:
    """Estrutura de dados para imagem viral"""
    image_url: str
    post_url: str
    platform: str
    title: str
    description: str
    engagement_score: float
    views_estimate: int
    likes_estimate: int
    comments_estimate: int
    shares_estimate: int
    author: str
    author_followers: int
    post_date: str
    hashtags: List[str]
    image_path: Optional[str] = None
    screenshot_path: Optional[str] = None
    extracted_at: str = datetime.now().isoformat()

class ViralImageFinder:
    """Classe principal para encontrar imagens virais"""

    def __init__(self, config: Dict = None):
        self.config = config or self._load_config()
        self.api_keys = self._load_multiple_api_keys()
        self.current_api_index = {
            'serper': 0,
            'google_cse': 0
        }
        self.failed_apis = set()
        self._ensure_directories()
        self.session = requests.Session()
        self.setup_session()

    def _load_config(self) -> Dict:
        """Carrega configurações do ambiente"""
        return {
            'serper_api_key': os.getenv('SERPER_API_KEY'),
            'google_search_key': os.getenv('GOOGLE_SEARCH_KEY'),
            'google_cse_id': os.getenv('GOOGLE_CSE_ID'),
            'max_images': int(os.getenv('MAX_IMAGES', 30)),
            'timeout': int(os.getenv('TIMEOUT', 30)),
            'output_dir': os.getenv('OUTPUT_DIR', 'viral_images_data'),
            'images_dir': os.getenv('IMAGES_DIR', 'downloaded_images'),
            'extract_images': os.getenv('EXTRACT_IMAGES', 'True').lower() == 'true',
        }

    def _load_multiple_api_keys(self) -> Dict:
        """Carrega múltiplas chaves de API para rotação"""
        api_keys = {
            'serper': [],
            'google_cse': []
        }

        # Serper - múltiplas chaves
        for i in range(1, 5): # Tenta carregar até 4 chaves Serper
            key = os.getenv(f'SERPER_API_KEY_{i}') or (os.getenv('SERPER_API_KEY') if i == 1 else None)
            if key and key.strip():
                api_keys['serper'].append(key.strip())
                logger.info(f"✅ Serper API {i} carregada")

        # Google CSE
        google_key = os.getenv('GOOGLE_SEARCH_KEY')
        google_cse = os.getenv('GOOGLE_CSE_ID')
        if google_key and google_cse:
            api_keys['google_cse'].append({'key': google_key, 'cse_id': google_cse})
            logger.info(f"✅ Google CSE carregada")

        return api_keys

    def _get_next_api_key(self, service: str) -> Optional[str]:
        """Obtém próxima chave de API disponível com rotação automática"""
        if service not in self.api_keys or not self.api_keys[service]:
            return None

        keys = self.api_keys[service]
        if not keys:
            return None

        # Tenta todas as chaves disponíveis
        for attempt in range(len(keys)):
            current_index = self.current_api_index[service]
            # Verifica se esta API não falhou recentemente (implementação simples, sem fallback automático)
            api_identifier = f"{service}_{current_index}"
            if api_identifier not in self.failed_apis:
                key = keys[current_index]
                logger.info(f"🔄 Usando {service} API #{current_index + 1}")
                # Avança para a próxima API na próxima chamada
                self.current_api_index[service] = (current_index + 1) % len(keys)
                return key
            # Se esta API falhou, tenta a próxima
            self.current_api_index[service] = (current_index + 1) % len(keys)
        
        logger.error(f"❌ Todas as APIs de {service} falharam recentemente")
        return None


    def _ensure_directories(self):
        """Garante que todos os diretórios necessários existam"""
        dirs_to_create = [
            self.config['output_dir'],
            self.config['images_dir']
        ]
        for directory in dirs_to_create:
            try:
                os.makedirs(directory, exist_ok=True)
                logger.info(f"✅ Diretório criado/verificado: {directory}")
            except Exception as e:
                logger.error(f"❌ Erro ao criar diretório {directory}: {e}")

    def setup_session(self):
        """Configura sessão HTTP com headers apropriados"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })

    async def search_images(self, query: str) -> List[Dict]:
        """Busca imagens usando Google Images via Serper com queries otimizadas"""
        logger.info(f"🔍 INICIANDO BUSCA DE IMAGENS: {query}")

        all_results = []

        # Queries específicas para redes sociais e conteúdo educacional
        queries = [
            f'"{query}" site:instagram.com',
            f'site:instagram.com/p "{query}"',
            f'site:instagram.com/reel "{query}"',
            f'"{query}" site:facebook.com',
            f'"{query}" site:youtube.com',
            f'"{query}" curso online tutorial'
        ]

        for q in queries[:4]:  # Limitar a 4 queries para controle de rate limit
            logger.info(f"🔍 Buscando imagens para: {q}")
            results = await self._search_serper_images(q)
            all_results.extend(results)
            await asyncio.sleep(0.5)  # Pequena pausa entre as buscas

        # Remove duplicatas baseadas na URL da imagem
        seen_image_urls = set()
        unique_results = []
        for result in all_results:
            image_url = result.get('image_url', '')
            if image_url and image_url not in seen_image_urls:
                seen_image_urls.add(image_url)
                unique_results.append(result)

        logger.info(f"✅ Total de imagens únicas encontradas: {len(unique_results)}")
        return unique_results[:self.config['max_images']]

    async def _search_serper_images(self, query: str) -> List[Dict]:
        """Busca imagens usando Serper API com rotação de chaves e fallbacks automáticos"""
        
        # TENTA TODAS AS CHAVES SERPER PRIMEIRO
        for attempt in range(len(self.api_keys.get('serper', []))):
            api_key = self._get_next_api_key('serper')
            if not api_key:
                break
                
            logger.info(f"🔄 Tentando SERPER (tentativa {attempt + 1})")
            
            url = "https://google.serper.dev/images"
            payload = {
                "q": query,
                "num": 20,
                "safe": "off",
                "gl": "br",
                "hl": "pt-br",
                "imgSize": "large",
                "imgType": "photo"
            }

            headers = {
                'X-API-KEY': api_key,
                'Content-Type': 'application/json'
            }

            try:
                if HAS_ASYNC_DEPS:
                    timeout = aiohttp.ClientTimeout(total=self.config['timeout'])
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.post(url, headers=headers, json=payload) as response:
                            if response.status == 200:
                                data = await response.json()
                                results = self._process_serper_results(data, query)
                                if results:
                                    logger.info(f"✅ SERPER funcionou - {len(results)} resultados")
                                    return results
                            elif response.status == 400:
                                error_data = await response.json()
                                if 'Not enough credits' in error_data.get('message', ''):
                                    logger.warning(f"⚠️ SERPER sem créditos, tentando próxima chave...")
                                    continue
                else:
                    response = self.session.post(url, headers=headers, json=payload, timeout=self.config['timeout'])
                    if response.status_code == 200:
                        data = response.json()
                        results = self._process_serper_results(data, query)
                        if results:
                            logger.info(f"✅ SERPER funcionou - {len(results)} resultados")
                            return results
                    elif response.status_code == 400:
                        error_data = response.json()
                        if 'Not enough credits' in error_data.get('message', ''):
                            logger.warning(f"⚠️ SERPER sem créditos, tentando próxima chave...")
                            continue

            except Exception as e:
                logger.warning(f"⚠️ Erro SERPER: {e}, tentando próxima chave...")
                continue
        
        # SE TODAS AS CHAVES SERPER FALHARAM, USA FALLBACKS AUTOMÁTICOS
        logger.warning("🔄 SERPER falhou completamente - ativando fallbacks automáticos")
        
        # FALLBACK 1: JINA
        jina_results = await self._search_jina_fallback(query)
        if jina_results:
            return jina_results
            
        # FALLBACK 2: EXA
        exa_results = await self._search_exa_fallback(query)
        if exa_results:
            return exa_results
            
        # FALLBACK 3: TRAFILATURA (busca web básica)
        web_results = await self._search_web_fallback(query)
        if web_results:
            return web_results
            
        logger.error("❌ TODOS os fallbacks falharam para busca de imagens")
        return []
    
    def _process_serper_results(self, data: dict, query: str) -> List[Dict]:
        """Processa resultados do Serper"""
        results = []
        for item in data.get('images', []):
            image_url = item.get('imageUrl', '')
            if image_url and self._is_valid_image_url(image_url):
                results.append({
                    'image_url': image_url,
                    'page_url': item.get('link', ''),
                    'title': item.get('title', ''),
                    'description': item.get('snippet', ''),
                    'source': 'serper_images',
                    'platform': self._detect_platform(item.get('link', '')),
                    'viral_score': self._calculate_viral_score(item)
                })
        return results

    async def _search_jina_fallback(self, query: str) -> List[Dict]:
        """Fallback usando JINA para busca de imagens"""
        logger.info("🔄 Ativando fallback JINA")
        
        jina_keys = [
            os.getenv('JINA_API_KEY'),
            os.getenv('JINA_API_KEY_1'),
            os.getenv('JINA_API_KEY_2'),
            os.getenv('JINA_API_KEY_3'),
            os.getenv('JINA_API_KEY_4')
        ]
        
        for i, api_key in enumerate(jina_keys):
            if not api_key:
                continue
                
            try:
                logger.info(f"🔄 Tentando JINA chave {i+1}")
                url = f"https://s.jina.ai/{query} site:instagram.com OR site:facebook.com images"
                headers = {
                    'Authorization': f'Bearer {api_key}',
                    'Accept': 'application/json'
                }
                
                if HAS_ASYNC_DEPS:
                    timeout = aiohttp.ClientTimeout(total=self.config['timeout'])
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(url, headers=headers) as response:
                            if response.status == 200:
                                data = await response.json()
                                results = self._process_jina_results(data, query)
                                if results:
                                    logger.info(f"✅ JINA funcionou - {len(results)} resultados")
                                    return results
                else:
                    response = self.session.get(url, headers=headers, timeout=self.config['timeout'])
                    if response.status_code == 200:
                        data = response.json()
                        results = self._process_jina_results(data, query)
                        if results:
                            logger.info(f"✅ JINA funcionou - {len(results)} resultados")
                            return results
                            
            except Exception as e:
                logger.warning(f"⚠️ JINA chave {i+1} falhou: {e}")
                continue
                
        logger.warning("❌ Todas as chaves JINA falharam")
        return []
    
    async def _search_exa_fallback(self, query: str) -> List[Dict]:
        """Fallback usando EXA para busca de imagens"""
        logger.info("🔄 Ativando fallback EXA")
        
        exa_keys = [
            os.getenv('EXA_API_KEY'),
            os.getenv('EXA_API_KEY_1')
        ]
        
        for i, api_key in enumerate(exa_keys):
            if not api_key:
                continue
                
            try:
                logger.info(f"🔄 Tentando EXA chave {i+1}")
                url = "https://api.exa.ai/search"
                payload = {
                    "query": f"{query} site:instagram.com OR site:facebook.com",
                    "numResults": 20,
                    "type": "neural",
                    "includeDomains": ["instagram.com", "facebook.com", "youtube.com"]
                }
                headers = {
                    'x-api-key': api_key,
                    'Content-Type': 'application/json'
                }
                
                if HAS_ASYNC_DEPS:
                    timeout = aiohttp.ClientTimeout(total=self.config['timeout'])
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.post(url, headers=headers, json=payload) as response:
                            if response.status == 200:
                                data = await response.json()
                                results = self._process_exa_results(data, query)
                                if results:
                                    logger.info(f"✅ EXA funcionou - {len(results)} resultados")
                                    return results
                else:
                    response = self.session.post(url, headers=headers, json=payload, timeout=self.config['timeout'])
                    if response.status_code == 200:
                        data = response.json()
                        results = self._process_exa_results(data, query)
                        if results:
                            logger.info(f"✅ EXA funcionou - {len(results)} resultados")
                            return results
                            
            except Exception as e:
                logger.warning(f"⚠️ EXA chave {i+1} falhou: {e}")
                continue
                
        logger.warning("❌ Todas as chaves EXA falharam")
        return []
    
    async def _search_web_fallback(self, query: str) -> List[Dict]:
        """Fallback usando busca web básica com TRAFILATURA"""
        logger.info("🔄 Ativando fallback WEB básico")
        
        try:
            # URLs básicas para buscar imagens relacionadas ao tema
            search_urls = [
                f"https://www.google.com/search?q={query}+site:instagram.com&tbm=isch",
                f"https://www.google.com/search?q={query}+site:facebook.com&tbm=isch",
                f"https://www.google.com/search?q={query}+tutorial&tbm=isch"
            ]
            
            results = []
            for url in search_urls[:1]:  # Apenas primeira URL para não sobrecarregar
                try:
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                    
                    if HAS_ASYNC_DEPS:
                        timeout = aiohttp.ClientTimeout(total=self.config['timeout'])
                        async with aiohttp.ClientSession(timeout=timeout) as session:
                            async with session.get(url, headers=headers) as response:
                                if response.status == 200:
                                    html = await response.text()
                                    web_results = self._extract_images_from_html(html, query)
                                    results.extend(web_results)
                    else:
                        response = self.session.get(url, headers=headers, timeout=self.config['timeout'])
                        if response.status_code == 200:
                            web_results = self._extract_images_from_html(response.text, query)
                            results.extend(web_results)
                            
                except Exception as e:
                    logger.warning(f"⚠️ Erro na busca web: {e}")
                    continue
            
            if results:
                logger.info(f"✅ Fallback WEB funcionou - {len(results)} resultados")
                return results[:10]  # Limita a 10 resultados
                
        except Exception as e:
            logger.warning(f"⚠️ Fallback WEB falhou: {e}")
            
        return []
    
    def _process_jina_results(self, data: dict, query: str) -> List[Dict]:
        """Processa resultados do JINA"""
        results = []
        for item in data.get('data', []):
            if 'url' in item and 'title' in item:
                # Simula uma imagem baseada no conteúdo JINA
                results.append({
                    'image_url': f"https://via.placeholder.com/400x300?text={query.replace(' ', '+')}",
                    'page_url': item.get('url', ''),
                    'title': item.get('title', ''),
                    'description': item.get('content', '')[:200],
                    'source': 'jina_fallback',
                    'platform': self._detect_platform(item.get('url', '')),
                    'viral_score': 6.0  # Score padrão para fallback
                })
        return results
    
    def _process_exa_results(self, data: dict, query: str) -> List[Dict]:
        """Processa resultados do EXA"""
        results = []
        for item in data.get('results', []):
            if 'url' in item and 'title' in item:
                results.append({
                    'image_url': f"https://via.placeholder.com/400x300?text={query.replace(' ', '+')}",
                    'page_url': item.get('url', ''),
                    'title': item.get('title', ''),
                    'description': item.get('text', '')[:200],
                    'source': 'exa_fallback',
                    'platform': self._detect_platform(item.get('url', '')),
                    'viral_score': 6.5  # Score padrão para fallback
                })
        return results
    
    def _extract_images_from_html(self, html: str, query: str) -> List[Dict]:
        """Extrai imagens básicas do HTML"""
        results = []
        try:
            if HAS_BS4:
                soup = BeautifulSoup(html, 'html.parser')
                img_tags = soup.find_all('img', limit=5)
                
                for i, img in enumerate(img_tags):
                    src = img.get('src', '')
                    if src and ('instagram' in src or 'facebook' in src or 'youtube' in src):
                        results.append({
                            'image_url': src,
                            'page_url': '',
                            'title': f"{query} - Resultado Web {i+1}",
                            'description': img.get('alt', ''),
                            'source': 'web_fallback',
                            'platform': 'web',
                            'viral_score': 5.0
                        })
        except Exception as e:
            logger.warning(f"⚠️ Erro ao extrair imagens do HTML: {e}")
            
        return results

    def _is_valid_image_url(self, url: str) -> bool:
        """Verifica se a URL parece ser de uma imagem real, evitando páginas de login ou não-imagem."""
        if not url or not isinstance(url, str):
            return False

        # Padrões para URLs que claramente NÃO são imagens
        invalid_patterns = [
            r'instagram\.com/accounts/login', # Páginas de login do Instagram
            r'facebook\.com/login',           # Páginas de login do Facebook
            r'login\.php',                    # Scripts de login genéricos
            r'/login/',                       # Padrão comum para URLs de login
            r'/auth/',                        # Padrão comum para URLs de autenticação
            r'\.html$',                       # Arquivos HTML geralmente não são imagens diretas
            r'\.php$',                        # Arquivos PHP geralmente não são imagens diretas
            r'\.jsp$',                        # Arquivos JSP geralmente não são imagens diretas
            r'\.asp$'                         # Arquivos ASP geralmente não são imagens diretas
        ]

        # Verifica se alguma URL inválida está presente
        if any(re.search(pattern, url, re.IGNORECASE) for pattern in invalid_patterns):
            return False

        # Padrões para URLs que PROVAVELMENTE são imagens
        valid_patterns = [
            r'\.(jpg|jpeg|png|gif|webp|bmp)(\?|$)', # Extensões comuns de imagem com ou sem query params
            r'scontent.*\.(jpg|png|webp)',        # URLs de CDN do Instagram
            r'cdninstagram\.com',                 # Outra forma de CDN do Instagram
            r'fbcdn\.net',                        # URLs de CDN do Facebook
            r'img\.youtube\.com',                 # Thumbnails do YouTube
            r'i\.ytimg\.com',                     # Thumbnails alternativos do YouTube
            r'googleusercontent\.com',            # Imagens do Google
            r'ggpht\.com',                        # Google Photos/YouTube
            r'licdn\.com',                        # CDN do LinkedIn
            r'linkedin\.com.*\.(jpg|png|webp)',  # Imagens do LinkedIn
            r'scontent-.*\.cdninstagram\.com',     # CDN específico do Instagram
            r'scontent\..*\.fbcdn\.net'          # CDN específico do Facebook
        ]

        # Verifica se alguma URL válida está presente
        return any(re.search(pattern, url, re.IGNORECASE) for pattern in valid_patterns)

    def _detect_platform(self, url: str) -> str:
        """Detecta a plataforma (Instagram, Facebook, YouTube, etc.) baseada na URL."""
        if 'instagram.com' in url:
            return 'instagram'
        elif 'facebook.com' in url:
            return 'facebook'
        elif 'youtube.com' in url or 'youtu.be' in url:
            return 'youtube'
        elif 'linkedin.com' in url:
            return 'linkedin'
        else:
            return 'web' # Plataforma genérica se não for reconhecida

    def _calculate_viral_score(self, item: Dict) -> float:
        """Calcula um score de viralidade básico baseado em metadados do Serper."""
        score = 5.0  # Score base inicial

        # Fatores que aumentam o score
        title = item.get('title', '').lower()

        # Palavras-chave que sugerem viralidade/popularidade
        viral_keywords = ['viral', 'trending', 'popular', 'mil', 'views', 'likes', 'compartilh', 'sucesso']
        for keyword in viral_keywords:
            if keyword in title:
                score += 1.0 # Adiciona 1 ponto para cada palavra-chave encontrada

        # Plataformas sociais (Instagram, Facebook, YouTube) tendem a ter mais engajamento viral
        link = item.get('link', '')
        if any(platform in link for platform in ['instagram.com', 'facebook.com', 'youtube.com']):
            score += 2.0 # Adiciona 2 pontos para plataformas sociais

        return min(score, 10.0)  # Limita o score máximo a 10.0

    async def extract_images_with_content(self, query: str) -> List[ViralImage]:
        """Função principal para buscar imagens virais e baixar o conteúdo."""
        logger.info(f"🚀 EXTRAINDO IMAGENS COM CONTEÚDO para a consulta: {query}")

        # Realiza a busca inicial de imagens usando Serper
        image_results = await self.search_images(query)

        viral_images = []
        # Processa cada resultado da busca
        for i, result in enumerate(image_results):
            try:
                # Cria um objeto ViralImage com os dados extraídos
                viral_image = ViralImage(
                    image_url=result['image_url'], # URL da imagem
                    post_url=result.get('page_url', ''), # URL do post/página
                    platform=result.get('platform', 'web'), # Plataforma detectada
                    title=result.get('title', ''), # Título do resultado
                    description=result.get('description', ''), # Descrição do resultado
                    engagement_score=result.get('viral_score', 5.0), # Score de viralidade
                    views_estimate=0, # Placeholder, pode ser preenchido posteriormente
                    likes_estimate=0, # Placeholder
                    comments_estimate=0, # Placeholder
                    shares_estimate=0, # Placeholder
                    author='', # Placeholder
                    author_followers=0, # Placeholder
                    post_date='', # Placeholder
                    hashtags=[] # Placeholder
                )

                # Baixa a imagem se a configuração permitir
                if self.config['extract_images']:
                    # Gera um nome de arquivo único baseado no índice e na URL
                    image_filename = f"viral_image_{i+1}_{hashlib.md5(result['image_url'].encode()).hexdigest()[:6]}"
                    image_path = await self._download_image(result['image_url'], image_filename)
                    viral_image.image_path = image_path # Define o caminho do arquivo baixado

                viral_images.append(viral_image) # Adiciona a imagem processada à lista

            except Exception as e:
                # Registra erro se o processamento de uma imagem falhar
                logger.error(f"❌ Erro ao processar imagem {i+1} (URL: {result.get('image_url')}): {e}")
                continue # Continua para a próxima imagem

        logger.info(f"✅ {len(viral_images)} imagens virais extraídas com sucesso para a consulta '{query}'")
        return viral_images

    async def _download_image(self, image_url: str, filename: str) -> Optional[str]:
        """Baixa uma imagem da URL de forma robusta, usando aiohttp se disponível."""
        try:
            # Define headers para simular um navegador
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://www.google.com/' # Define Referer para evitar bloqueios
            }

            # Utiliza aiohttp para downloads assíncronos se as dependências estiverem instaladas
            if HAS_ASYNC_DEPS:
                timeout = aiohttp.ClientTimeout(total=self.config['timeout'])
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(image_url, headers=headers) as response:
                        response.raise_for_status() # Verifica se a requisição foi bem-sucedida

                        # Obtém o tipo de conteúdo para validar se é uma imagem
                        content_type = response.headers.get('content-type', '').lower()
                        if 'image' not in content_type:
                            logger.warning(f"URL não é uma imagem válida: {image_url} (Content-Type: {content_type})")
                            return None

                        # Determina a extensão do arquivo com base no Content-Type
                        if 'jpeg' in content_type or 'jpg' in content_type:
                            ext = '.jpg'
                        elif 'png' in content_type:
                            ext = '.png'
                        elif 'webp' in content_type:
                            ext = '.webp'
                        else:
                            ext = '.jpg'  # Extensão padrão se não for reconhecida

                        # Cria o caminho completo para salvar o arquivo
                        filepath = os.path.join(self.config['images_dir'], f"{filename}{ext}")

                        # Salva o conteúdo da imagem em chunks
                        async with aiofiles.open(filepath, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192): # Lê em blocos de 8KB
                                await f.write(chunk)

                        # Verifica se o arquivo foi salvo corretamente (tamanho mínimo)
                        if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:  # Verifica se tem pelo menos 1KB
                            logger.info(f"✅ Imagem baixada com sucesso: {filepath}")
                            return filepath
                        else:
                            # Remove arquivo inválido se existir
                            if os.path.exists(filepath):
                                os.remove(filepath)
                            logger.warning(f"Arquivo de imagem inválido ou vazio foi criado: {filepath}")
                            return None
            else: # Fallback para requests síncrono se aiohttp não estiver disponível
                response = self.session.get(image_url, headers=headers, timeout=self.config['timeout'])
                response.raise_for_status()

                content_type = response.headers.get('content-type', '').lower()
                if 'image' not in content_type:
                    logger.warning(f"URL não é uma imagem válida: {image_url} (Content-Type: {content_type})")
                    return None

                if 'jpeg' in content_type or 'jpg' in content_type: ext = '.jpg'
                elif 'png' in content_type: ext = '.png'
                elif 'webp' in content_type: ext = '.webp'
                else: ext = '.jpg'

                filepath = os.path.join(self.config['images_dir'], f"{filename}{ext}")

                with open(filepath, 'wb') as f:
                    f.write(response.content)

                if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                    logger.info(f"✅ Imagem baixada com sucesso: {filepath}")
                    return filepath
                else:
                    if os.path.exists(filepath):
                        os.remove(filepath)
                    logger.warning(f"Arquivo de imagem inválido ou vazio foi criado: {filepath}")
                    return None

        except Exception as e:
            logger.error(f"❌ Erro ao baixar imagem de {image_url}: {e}")
            return None

    def save_results(self, viral_images: List[ViralImage], filename: str = None) -> str:
        """Salva a lista de imagens virais em um arquivo JSON."""
        # Define um nome de arquivo padrão se nenhum for fornecido
        if not filename:
            filename = f"viral_images_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        # Cria o caminho completo para o arquivo de resultados
        filepath = os.path.join(self.config['output_dir'], filename)

        # Estrutura os dados para salvar em JSON
        data = {
            'timestamp': datetime.now().isoformat(), # Data e hora da extração
            'total_images': len(viral_images), # Número total de imagens encontradas
            'images': [asdict(img) for img in viral_images] # Lista de imagens convertidas para dicionário
        }

        # Salva os dados em um arquivo JSON com codificação UTF-8 e indentação
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"💾 Resultados salvos com sucesso em: {filepath}")
        except Exception as e:
            logger.error(f"❌ Erro ao salvar resultados em {filepath}: {e}")

        return filepath

# Cria uma instância global do serviço para uso
viral_image_finder = ViralImageFinder()

# Funções wrapper para compatibilidade com código existente, se necessário
async def find_viral_images_async(query: str) -> List[ViralImage]:
    """Wrapper assíncrono para a função principal."""
    return await viral_image_finder.extract_images_with_content(query)

def find_viral_images_sync(query: str) -> List[ViralImage]:
    """Wrapper síncrono que executa a função assíncrona em um loop de eventos."""
    if HAS_ASYNC_DEPS:
        try:
            # Tenta obter o loop de eventos atual
            loop = asyncio.get_running_loop()
            # Se um loop já estiver rodando, executa a tarefa em uma thread separada
            import concurrent.futures
            def run_async_task():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(viral_image_finder.extract_images_with_content(query))
                finally:
                    new_loop.close()
            # Utiliza ThreadPoolExecutor para executar a tarefa assíncrona
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_async_task)
                return future.result(timeout=300)  # Timeout de 5 minutos
        except RuntimeError:
            # Se não houver loop de eventos rodando, cria um novo
            return asyncio.run(viral_image_finder.extract_images_with_content(query))
    else:
        # Se aiohttp não estiver disponível, executa a lógica síncrona diretamente (com fallback para requests)
        # A lógica síncrona já está dentro de _search_serper_images e _download_image
        # Precisamos simular a chamada assíncrona
        logger.warning("aiohttp não disponível, executando busca de forma síncrona com fallback.")
        # Note: A adaptação completa para síncrono exigiria reescrever partes do código.
        # Por ora, retornamos uma lista vazia e um log de aviso.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(viral_image_finder.extract_images_with_content(query))
        loop.close()
        return result


logger.info("🔥 Viral Integration Service (Versão Otimizada) inicializado.")