# ARQV30 Enhanced Market Analysis System

## Overview

ARQV30 Enhanced is a comprehensive market analysis system that generates ultra-detailed market intelligence reports. The system uses multiple AI providers (Gemini, OpenAI, Groq, HuggingFace) and various data sources to create in-depth analysis including competitive intelligence, audience avatars, mental drivers, and predictive analytics. It operates as a Flask web application with a sophisticated data collection and processing pipeline.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Backend Architecture
- **Framework**: Flask 3.0.0 with Gunicorn for production deployment
- **Language**: Python 3.x with extensive use of async/await patterns
- **File Structure**: Modular design with separate services, routes, and engines
- **Data Processing**: Multi-threaded processing with ThreadPoolExecutor for concurrent operations

### AI Integration Layer
- **Primary AI**: Google Gemini 2.0 Flash (configured as primary model)
- **Fallback Providers**: OpenAI GPT, Groq (llama3-70b-8192), HuggingFace models
- **AI Manager**: Intelligent provider rotation with automatic fallback on failures
- **Enhanced Features**: Tool use support, active web search capabilities, function calling

### Data Collection System
- **Web Scraping**: Selenium WebDriver with automated browser management
- **Content Extraction**: Multiple extraction strategies (BeautifulSoup, Readability, Trafilatura)
- **Search APIs**: Google Custom Search, Exa AI, Jina AI for content extraction
- **Social Media**: Integrated social media data collection from multiple platforms

### Analysis Pipeline
- **Three-Phase Methodology**: 
  1. Massive data collection with screenshot capture
  2. AI synthesis with active tool use
  3. Comprehensive report generation
- **Module Processing**: 26+ specialized analysis modules including CPL protocols, competitor analysis, avatar generation
- **Quality Validation**: Content quality thresholds and validation layers

### Data Storage
- **Primary**: Local file system with JSON-based storage
- **Structure**: Organized directory hierarchy for different analysis components
- **Auto-Save**: Continuous saving system with error recovery
- **Sessions**: Session-based data management with unique identifiers

### Report Generation
- **Formats**: HTML and JSON report generation
- **Minimum Standards**: 20+ page reports with comprehensive content
- **Professional Templates**: Modern responsive HTML templates
- **Data Visualization**: Integrated charts and graphs support

### Security & Configuration
- **Environment Management**: Robust .env file handling with validation
- **API Keys**: Centralized API key management with health checking
- **Error Handling**: Comprehensive error logging and recovery mechanisms
- **Production Ready**: Debug disabled, proper logging configuration

### Real-Time Features
- **Progress Tracking**: WebSocket-based real-time progress updates
- **Session Management**: Flask sessions with unique session IDs
- **Auto-Save**: Background saving during long-running analyses

## External Dependencies

### Core AI Services
- **Google Gemini AI**: Primary AI provider with 2.0 Flash model
- **OpenAI API**: Secondary AI provider for analysis tasks
- **Groq API**: High-speed inference for specific use cases
- **HuggingFace API**: Additional NLP capabilities

### Search & Data APIs
- **Google Custom Search API**: Web search functionality (requires GOOGLE_API_KEY, GOOGLE_CSE_ID)
- **Exa AI**: Neural search capabilities
- **Jina AI**: Content extraction and reading (r.jina.ai)
- **Tavily API**: Additional search capabilities

### Social Media APIs
- **YouTube Data API v3**: Video content analysis
- **Twitter API**: Social media insights
- **Instagram Graph API**: Visual content analysis
- **LinkedIn API**: Professional network data

### Browser Automation
- **Selenium WebDriver**: Automated web scraping
- **Playwright**: Modern browser automation for screenshots
- **WebDriver Manager**: Automatic driver management

### Document Processing
- **PyPDF2**: PDF document analysis
- **python-docx**: Word document processing  
- **openpyxl**: Excel spreadsheet analysis
- **Pillow**: Image processing capabilities
- **pytesseract**: OCR text extraction

### Database & Storage
- **Supabase**: Cloud database integration (optional, fallback to local)
- **Local JSON**: File-based storage system as primary

### Web Infrastructure
- **Flask-SocketIO**: Real-time communication
- **Flask-CORS**: Cross-origin resource sharing
- **Gunicorn**: WSGI HTTP Server for production
- **Requests/HTTPX/aiohttp**: HTTP client libraries

### Development Tools
- **python-dotenv**: Environment variable management
- **configparser**: Configuration file handling
- **logging**: Comprehensive logging system
- **concurrent.futures**: Multi-threading support