# services/viral_integration_service.py
"""VIRAL INTEGRATION SERVICE - ARQV30 Enhanced v3.0
Integra dados virais coletados na primeira etapa para a segunda etapa
Processa viral_results e viral_search para alimentar análise profunda
"""
import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)

class ViralIntegrationService:
    """Serviço para integrar dados virais entre etapas"""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.base_path = Path("analyses_data")
        self.viral_path = self.base_path / "viral_content" / session_id
        self.pesquisa_path = self.base_path / "pesquisa_web" / session_id
        self.output_path = self.base_path / "integrated_data" / session_id
        
        # Criar diretórios necessários
        self.output_path.mkdir(parents=True, exist_ok=True)
        
    def collect_viral_data(self) -> Dict[str, Any]:
        """Coleta todos os dados virais disponíveis"""
        viral_data = {
            "viral_results": [],
            "viral_search": [],
            "viral_images": [],
            "viral_posts": [],
            "viral_analytics": {},
            "collection_timestamp": datetime.now().isoformat()
        }
        
        try:
            # Coletar viral_results
            viral_results = self._collect_viral_results()
            viral_data["viral_results"] = viral_results
            logger.info(f"✅ Coletados {len(viral_results)} viral_results")
            
            # Coletar viral_search
            viral_search = self._collect_viral_search()
            viral_data["viral_search"] = viral_search
            logger.info(f"✅ Coletados {len(viral_search)} viral_search")
            
            # Coletar imagens virais
            viral_images = self._collect_viral_images()
            viral_data["viral_images"] = viral_images
            logger.info(f"✅ Coletadas {len(viral_images)} viral_images")
            
            # Coletar posts virais
            viral_posts = self._collect_viral_posts()
            viral_data["viral_posts"] = viral_posts
            logger.info(f"✅ Coletados {len(viral_posts)} viral_posts")
            
            # Gerar analytics
            viral_analytics = self._generate_viral_analytics(viral_data)
            viral_data["viral_analytics"] = viral_analytics
            logger.info(f"✅ Analytics gerados: {len(viral_analytics)} métricas")
            
        except Exception as e:
            logger.error(f"❌ Erro ao coletar dados virais: {e}")
            
        return viral_data
    
    def _collect_viral_results(self) -> List[Dict]:
        """Coleta arquivos viral_results_*.json"""
        results = []
        
        # Procurar em viral_content
        if self.viral_path.exists():
            for file_path in self.viral_path.glob("viral_results_*.json"):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            results.extend(data)
                        elif isinstance(data, dict):
                            results.append(data)
                    logger.info(f"📄 Carregado viral_results: {file_path.name}")
                except Exception as e:
                    logger.error(f"❌ Erro ao carregar {file_path}: {e}")
        
        # Procurar em pesquisa_web também
        if self.pesquisa_path.exists():
            for file_path in self.pesquisa_path.glob("viral_results_*.json"):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            results.extend(data)
                        elif isinstance(data, dict):
                            results.append(data)
                    logger.info(f"📄 Carregado viral_results de pesquisa: {file_path.name}")
                except Exception as e:
                    logger.error(f"❌ Erro ao carregar {file_path}: {e}")
                    
        return results
    
    def _collect_viral_search(self) -> List[Dict]:
        """Coleta arquivos viral_search_*.json"""
        results = []
        
        # Procurar em viral_content
        if self.viral_path.exists():
            for file_path in self.viral_path.glob("viral_search_*.json"):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            results.extend(data)
                        elif isinstance(data, dict):
                            results.append(data)
                    logger.info(f"📄 Carregado viral_search: {file_path.name}")
                except Exception as e:
                    logger.error(f"❌ Erro ao carregar {file_path}: {e}")
        
        # Procurar em pesquisa_web também
        if self.pesquisa_path.exists():
            for file_path in self.pesquisa_path.glob("viral_search_*.json"):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            results.extend(data)
                        elif isinstance(data, dict):
                            results.append(data)
                    logger.info(f"📄 Carregado viral_search de pesquisa: {file_path.name}")
                except Exception as e:
                    logger.error(f"❌ Erro ao carregar {file_path}: {e}")
                    
        return results
    
    def _collect_viral_images(self) -> List[Dict]:
        """Coleta dados de imagens virais"""
        images = []
        
        # Procurar arquivos de imagens
        for base_path in [self.viral_path, self.pesquisa_path]:
            if base_path.exists():
                for file_path in base_path.glob("*viral*image*.json"):
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            if isinstance(data, list):
                                images.extend(data)
                            elif isinstance(data, dict):
                                images.append(data)
                        logger.info(f"📸 Carregadas imagens virais: {file_path.name}")
                    except Exception as e:
                        logger.error(f"❌ Erro ao carregar imagens {file_path}: {e}")
                        
        return images
    
    def _collect_viral_posts(self) -> List[Dict]:
        """Coleta dados de posts virais"""
        posts = []
        
        # Procurar arquivos de posts
        for base_path in [self.viral_path, self.pesquisa_path]:
            if base_path.exists():
                for file_path in base_path.glob("*viral*post*.json"):
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            if isinstance(data, list):
                                posts.extend(data)
                            elif isinstance(data, dict):
                                posts.append(data)
                        logger.info(f"📝 Carregados posts virais: {file_path.name}")
                    except Exception as e:
                        logger.error(f"❌ Erro ao carregar posts {file_path}: {e}")
                        
        return posts
    
    def _generate_viral_analytics(self, viral_data: Dict) -> Dict:
        """Gera analytics dos dados virais coletados"""
        analytics = {
            "total_viral_results": len(viral_data.get("viral_results", [])),
            "total_viral_search": len(viral_data.get("viral_search", [])),
            "total_viral_images": len(viral_data.get("viral_images", [])),
            "total_viral_posts": len(viral_data.get("viral_posts", [])),
            "platforms_found": set(),
            "engagement_metrics": {
                "total_likes": 0,
                "total_comments": 0,
                "total_shares": 0,
                "average_engagement": 0
            },
            "content_types": {},
            "hashtags_frequency": {},
            "top_performers": []
        }
        
        # Analisar viral_results
        for result in viral_data.get("viral_results", []):
            if isinstance(result, dict):
                # Plataformas
                platform = result.get("platform", "unknown")
                analytics["platforms_found"].add(platform)
                
                # Métricas de engajamento
                likes = result.get("likes_estimate", 0) or result.get("likes", 0)
                comments = result.get("comments_estimate", 0) or result.get("comments", 0)
                shares = result.get("shares_estimate", 0) or result.get("shares", 0)
                
                analytics["engagement_metrics"]["total_likes"] += likes
                analytics["engagement_metrics"]["total_comments"] += comments
                analytics["engagement_metrics"]["total_shares"] += shares
                
                # Tipos de conteúdo
                content_type = result.get("content_type", "post")
                analytics["content_types"][content_type] = analytics["content_types"].get(content_type, 0) + 1
                
                # Hashtags
                hashtags = result.get("hashtags", [])
                for hashtag in hashtags:
                    analytics["hashtags_frequency"][hashtag] = analytics["hashtags_frequency"].get(hashtag, 0) + 1
                
                # Top performers
                engagement_score = result.get("engagement_score", 0)
                if engagement_score > 0:
                    analytics["top_performers"].append({
                        "url": result.get("post_url", ""),
                        "title": result.get("title", ""),
                        "engagement_score": engagement_score,
                        "platform": platform
                    })
        
        # Calcular média de engajamento
        total_items = len(viral_data.get("viral_results", []))
        if total_items > 0:
            total_engagement = (analytics["engagement_metrics"]["total_likes"] + 
                              analytics["engagement_metrics"]["total_comments"] + 
                              analytics["engagement_metrics"]["total_shares"])
            analytics["engagement_metrics"]["average_engagement"] = total_engagement / total_items
        
        # Ordenar top performers
        analytics["top_performers"] = sorted(
            analytics["top_performers"], 
            key=lambda x: x["engagement_score"], 
            reverse=True
        )[:10]
        
        # Converter set para list para JSON
        analytics["platforms_found"] = list(analytics["platforms_found"])
        
        return analytics
    
    def save_integrated_data(self, viral_data: Dict) -> str:
        """Salva dados integrados para a segunda etapa"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"viral_integrated_{timestamp}.json"
        filepath = self.output_path / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(viral_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ Dados virais integrados salvos: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"❌ Erro ao salvar dados integrados: {e}")
            return ""
    
    def create_summary_for_stage2(self, viral_data: Dict) -> Dict:
        """Cria resumo dos dados virais para a segunda etapa"""
        summary = {
            "session_id": self.session_id,
            "viral_data_summary": {
                "total_content_pieces": (
                    len(viral_data.get("viral_results", [])) + 
                    len(viral_data.get("viral_search", [])) + 
                    len(viral_data.get("viral_posts", []))
                ),
                "platforms_analyzed": viral_data.get("viral_analytics", {}).get("platforms_found", []),
                "top_engagement_content": viral_data.get("viral_analytics", {}).get("top_performers", [])[:5],
                "key_hashtags": list(viral_data.get("viral_analytics", {}).get("hashtags_frequency", {}).keys())[:10],
                "engagement_totals": viral_data.get("viral_analytics", {}).get("engagement_metrics", {}),
                "content_types_distribution": viral_data.get("viral_analytics", {}).get("content_types", {})
            },
            "recommendations_for_stage2": {
                "focus_platforms": viral_data.get("viral_analytics", {}).get("platforms_found", []),
                "high_engagement_patterns": self._identify_patterns(viral_data),
                "content_gaps": self._identify_content_gaps(viral_data),
                "viral_triggers": self._identify_viral_triggers(viral_data)
            },
            "data_quality": {
                "completeness_score": self._calculate_completeness(viral_data),
                "reliability_score": self._calculate_reliability(viral_data),
                "freshness_score": self._calculate_freshness(viral_data)
            },
            "created_at": datetime.now().isoformat()
        }
        
        return summary
    
    def _identify_patterns(self, viral_data: Dict) -> List[str]:
        """Identifica padrões de alto engajamento"""
        patterns = []
        
        # Analisar top performers
        top_performers = viral_data.get("viral_analytics", {}).get("top_performers", [])
        
        if len(top_performers) >= 3:
            # Padrões de plataforma
            platforms = [p.get("platform") for p in top_performers[:5]]
            most_common_platform = max(set(platforms), key=platforms.count) if platforms else None
            if most_common_platform:
                patterns.append(f"Alto engajamento em {most_common_platform}")
            
            # Padrões de título
            titles = [p.get("title", "") for p in top_performers[:5]]
            if any("tutorial" in title.lower() for title in titles):
                patterns.append("Conteúdo tutorial gera alto engajamento")
            if any("dica" in title.lower() for title in titles):
                patterns.append("Dicas práticas são populares")
            if any("passo a passo" in title.lower() for title in titles):
                patterns.append("Conteúdo passo-a-passo é viral")
        
        return patterns
    
    def _identify_content_gaps(self, viral_data: Dict) -> List[str]:
        """Identifica lacunas no conteúdo"""
        gaps = []
        
        content_types = viral_data.get("viral_analytics", {}).get("content_types", {})
        
        # Verificar tipos de conteúdo ausentes
        expected_types = ["video", "image", "carousel", "story", "reel"]
        missing_types = [t for t in expected_types if t not in content_types]
        
        for missing_type in missing_types:
            gaps.append(f"Falta de conteúdo tipo: {missing_type}")
        
        # Verificar plataformas ausentes
        platforms = viral_data.get("viral_analytics", {}).get("platforms_found", [])
        expected_platforms = ["instagram", "facebook", "youtube", "tiktok"]
        missing_platforms = [p for p in expected_platforms if p not in platforms]
        
        for missing_platform in missing_platforms:
            gaps.append(f"Ausência na plataforma: {missing_platform}")
        
        return gaps
    
    def _identify_viral_triggers(self, viral_data: Dict) -> List[str]:
        """Identifica gatilhos virais"""
        triggers = []
        
        # Analisar hashtags mais frequentes
        hashtags = viral_data.get("viral_analytics", {}).get("hashtags_frequency", {})
        top_hashtags = sorted(hashtags.items(), key=lambda x: x[1], reverse=True)[:5]
        
        for hashtag, freq in top_hashtags:
            if freq >= 3:
                triggers.append(f"Hashtag viral: {hashtag} (usado {freq}x)")
        
        # Analisar padrões de engajamento
        engagement = viral_data.get("viral_analytics", {}).get("engagement_metrics", {})
        avg_engagement = engagement.get("average_engagement", 0)
        
        if avg_engagement > 1000:
            triggers.append("Alto engajamento médio indica audiência ativa")
        
        return triggers
    
    def _calculate_completeness(self, viral_data: Dict) -> float:
        """Calcula score de completude dos dados"""
        total_items = (
            len(viral_data.get("viral_results", [])) + 
            len(viral_data.get("viral_search", [])) + 
            len(viral_data.get("viral_posts", []))
        )
        
        if total_items == 0:
            return 0.0
        elif total_items < 10:
            return 0.3
        elif total_items < 25:
            return 0.6
        elif total_items < 50:
            return 0.8
        else:
            return 1.0
    
    def _calculate_reliability(self, viral_data: Dict) -> float:
        """Calcula score de confiabilidade dos dados"""
        # Verificar se há dados de múltiplas fontes
        platforms = viral_data.get("viral_analytics", {}).get("platforms_found", [])
        
        if len(platforms) == 0:
            return 0.0
        elif len(platforms) == 1:
            return 0.4
        elif len(platforms) == 2:
            return 0.7
        else:
            return 1.0
    
    def _calculate_freshness(self, viral_data: Dict) -> float:
        """Calcula score de frescor dos dados"""
        # Assumir que dados foram coletados recentemente
        # Em implementação real, verificaria timestamps dos dados
        return 0.9
    
    def process_and_integrate(self) -> Dict:
        """Processo principal de integração"""
        logger.info(f"🔄 Iniciando integração de dados virais para sessão: {self.session_id}")
        
        # Coletar dados virais
        viral_data = self.collect_viral_data()
        
        # Salvar dados integrados
        filepath = self.save_integrated_data(viral_data)
        
        # Criar resumo para stage 2
        summary = self.create_summary_for_stage2(viral_data)
        
        # Salvar resumo
        summary_filepath = self.output_path / f"viral_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(summary_filepath, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ Resumo viral salvo: {summary_filepath}")
        except Exception as e:
            logger.error(f"❌ Erro ao salvar resumo: {e}")
        
        result = {
            "success": True,
            "viral_data": viral_data,
            "summary": summary,
            "files_created": [filepath, str(summary_filepath)],
            "total_content_pieces": summary["viral_data_summary"]["total_content_pieces"],
            "platforms_found": summary["viral_data_summary"]["platforms_analyzed"],
            "integration_timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"✅ Integração viral concluída: {result['total_content_pieces']} peças de conteúdo processadas")
        
        return result