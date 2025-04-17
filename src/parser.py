import asyncio
import logging
import os
import random
import time
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urlparse
import json

import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fake_useragent import UserAgent
from playwright.async_api import async_playwright, Browser, Page
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Модель базы данных
Base = declarative_base()

class ParsedSite(Base):
    __tablename__ = 'parsed_sites'
    
    id = Column(Integer, primary_key=True)
    url = Column(String(500), unique=True)
    title = Column(String(500))
    description = Column(Text)
    parsed_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(50))
    error = Column(Text, nullable=True)

class Parser:
    def __init__(self):
        self.ua = UserAgent()
        self.proxy_list = os.getenv('PROXY_LIST', '').split(',')
        self.delay_min = int(os.getenv('DELAY_MIN', 2))
        self.delay_max = int(os.getenv('DELAY_MAX', 5))
        self.max_pages = int(os.getenv('MAX_PAGES', 10))
        self.retry_count = int(os.getenv('RETRY_COUNT', 3))
        self.timeout = int(os.getenv('TIMEOUT', 30))
        self.num_threads = int(os.getenv('NUM_THREADS', 4))
        
        # Инициализация базы данных
        db_path = os.getenv('DB_PATH', 'parser_results.db')
        self.engine = create_engine(f'sqlite:///{db_path}')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
        # Создание директорий для логов и результатов
        os.makedirs('logs', exist_ok=True)
        os.makedirs('results', exist_ok=True)
        os.makedirs('reports', exist_ok=True)

    async def get_random_proxy(self) -> Optional[str]:
        """Получение случайного прокси из списка"""
        return random.choice(self.proxy_list) if self.proxy_list else None

    async def create_browser(self) -> Browser:
        """Создание браузера с настройками"""
        proxy = await self.get_random_proxy()
        browser_args = []
        
        if proxy:
            browser_args.extend([
                f'--proxy-server={proxy}',
                '--ignore-certificate-errors',
                '--no-sandbox',
                '--disable-setuid-sandbox'
            ])
        
        return await self.playwright.chromium.launch(
            headless=True,
            args=browser_args
        )

    async def parse_page(self, url: str) -> Dict:
        """Парсинг отдельной страницы"""
        browser = None
        page = None
        result = {
            'url': url,
            'title': '',
            'description': '',
            'status': 'error',
            'error': None
        }
        
        try:
            browser = await self.create_browser()
            page = await browser.new_page()
            
            # Установка User-Agent
            await page.set_extra_http_headers({
                'User-Agent': self.ua.random
            })
            
            # Настройка таймаутов
            page.set_default_timeout(self.timeout * 1000)
            
            # Загрузка страницы
            response = await page.goto(url, wait_until='networkidle')
            if not response.ok:
                raise Exception(f"HTTP {response.status}")
            
            # Получение контента
            content = await page.content()
            soup = BeautifulSoup(content, 'lxml')
            
            # Извлечение данных
            result.update({
                'title': soup.title.string if soup.title else '',
                'description': soup.find('meta', {'name': 'description'})['content'] 
                             if soup.find('meta', {'name': 'description'}) else '',
                'status': 'success'
            })
            
        except Exception as e:
            logger.error(f"Error parsing {url}: {str(e)}")
            result['error'] = str(e)
            
        finally:
            if page:
                await page.close()
            if browser:
                await browser.close()
                
        return result

    async def save_result(self, result: Dict):
        """Сохранение результата в базу данных"""
        try:
            site = ParsedSite(
                url=result['url'],
                title=result['title'],
                description=result['description'],
                status=result['status'],
                error=result['error']
            )
            self.session.add(site)
            self.session.commit()
            
            # Сохранение в JSON
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"results/result_{timestamp}_{urlparse(result['url']).netloc}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.error(f"Error saving result: {str(e)}")
            self.session.rollback()

    async def parse_urls(self, urls: List[str]):
        """Парсинг списка URL с использованием пула потоков"""
        async with async_playwright() as playwright:
            self.playwright = playwright
            tasks = []
            
            for url in urls:
                task = asyncio.create_task(self.parse_page(url))
                tasks.append(task)
                
                if len(tasks) >= self.num_threads:
                    completed = await asyncio.gather(*tasks)
                    for result in completed:
                        await self.save_result(result)
                    tasks = []
                    
                # Случайная задержка между запросами
                await asyncio.sleep(random.uniform(self.delay_min, self.delay_max))
            
            # Обработка оставшихся задач
            if tasks:
                completed = await asyncio.gather(*tasks)
                for result in completed:
                    await self.save_result(result)

    def generate_report(self):
        """Генерация отчета о результатах парсинга"""
        total = self.session.query(ParsedSite).count()
        successful = self.session.query(ParsedSite).filter_by(status='success').count()
        failed = total - successful
        
        report = f"""
        Parser Report
        ============
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        Statistics:
        - Total URLs processed: {total}
        - Successful: {successful}
        - Failed: {failed}
        - Success rate: {(successful/total*100):.2f}%
        
        Recent Errors:
        """
        
        recent_errors = self.session.query(ParsedSite)\
            .filter(ParsedSite.error.isnot(None))\
            .order_by(ParsedSite.parsed_at.desc())\
            .limit(5)
            
        for error in recent_errors:
            report += f"\n- {error.url}: {error.error}"
            
        # Сохранение отчета
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        with open(f'reports/report_{timestamp}.txt', 'w', encoding='utf-8') as f:
            f.write(report)
            
        return report

async def main():
    parser = Parser()
    
    # Пример использования
    urls = [
        'https://example.com',
        'https://example.org',
        # Добавьте свои URL здесь
    ]
    
    await parser.parse_urls(urls)
    report = parser.generate_report()
    print(report)

if __name__ == '__main__':
    asyncio.run(main())
