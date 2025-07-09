"""
Settings and configuration management for the Solana Smart Traders pipeline.
Loads configuration from YAML files and environment variables.
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    pool_size: int = 10
    max_overflow: int = 20
    pool_recycle_hours: int = 1
    connection_timeout_seconds: int = 30
    default_batch_size: int = 1000
    max_batch_size: int = 5000


@dataclass
class APIConfig:
    """API configuration settings."""
    birdeye_base_url: str = "https://public-api.birdeye.so"
    rate_limit_per_second: int = 2
    timeout_seconds: int = 30
    max_retries: int = 3
    backoff_factor: int = 2


@dataclass
class BronzeTokensConfig:
    """Bronze tokens task configuration."""
    token_limit: int = 50
    min_liquidity: float = 100000
    max_liquidity: float = 50000000
    min_volume_1h_usd: float = 50000
    min_price_change_2h_percent: float = 15.0
    min_price_change_24h_percent: float = 20.0
    api_pagination_limit: int = 50
    api_rate_limit_delay: float = 0.5
    batch_size: int = 10
    max_processing_time_minutes: int = 30


@dataclass
class SilverTokensConfig:
    """Silver tokens task configuration."""
    min_holders: int = 100
    min_volume_24h: float = 50000
    selection_ratio: float = 0.2
    max_tokens_selected: int = 20
    batch_size: int = 50
    score_weights: Dict[str, float] = field(default_factory=lambda: {
        "liquidity": 0.3,
        "volume_24h": 0.25,
        "price_change_24h": 0.2,
        "holder_count": 0.15,
        "trade_count_24h": 0.1
    })


@dataclass
class BronzeWhalesConfig:
    """Bronze whales task configuration."""
    top_holders_limit: int = 20
    min_holding_percentage: float = 0.5
    api_rate_limit_delay: float = 0.5
    max_tokens_per_run: int = 10
    batch_size: int = 5
    refetch_interval_hours: int = 72


@dataclass
class SilverWhalesConfig:
    """Silver whales task configuration."""
    min_tokens_held: int = 2
    min_total_value_usd: float = 10000
    exclude_cex_addresses: bool = True
    max_whales_selected: int = 100
    batch_size: int = 50
    excluded_addresses: list = field(default_factory=lambda: [
        "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",  # Raydium
        "7YttLkHDoNj9wyDur5pM1ejNaAvT9X4eqaYcHQqtj2G5",  # Serum DEX
        "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",  # Orca
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"   # Mango
    ])


@dataclass
class BronzeTransactionsConfig:
    """Bronze transactions task configuration."""
    max_transactions_per_wallet: int = 100
    lookback_days: int = 30
    wallet_api_delay: float = 1.0
    batch_size: int = 5
    max_wallets_per_run: int = 20
    refetch_interval_days: int = 30
    min_transaction_value_usd: float = 100
    include_transaction_types: list = field(default_factory=lambda: ["swap", "transfer"])


@dataclass
class SilverWalletPnLConfig:
    """Silver wallet PnL task configuration."""
    calculation_method: str = "fifo"
    include_unrealized: bool = False
    min_trades_for_calculation: int = 5
    wallet_batch_size: int = 10
    batch_size: int = 50
    max_processing_time_minutes: int = 60
    memory_limit_mb: int = 2048
    min_realized_pnl_usd: float = 100
    min_win_rate_percent: float = 30


@dataclass
class SmartTradersConfig:
    """Smart traders task configuration."""
    min_win_rate: float = 60.0
    min_realized_pnl: float = 1000.0
    min_trade_count: int = 10
    min_trading_days: int = 7
    batch_size: int = 50
    performance_tiers: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
        "elite": {"min_pnl": 10000, "min_roi": 50, "min_win_rate": 70},
        "strong": {"min_pnl": 5000, "min_roi": 25, "min_win_rate": 60},
        "promising": {"min_pnl": 1000, "min_roi": 15, "min_win_rate": 50},
        "qualified": {"min_pnl": 100, "min_roi": 5, "min_win_rate": 40}
    })
    scoring_weights: Dict[str, float] = field(default_factory=lambda: {
        "realized_pnl": 0.4,
        "roi_percentage": 0.3,
        "win_rate": 0.2,
        "trade_frequency": 0.1
    })


@dataclass
class MonitoringConfig:
    """Monitoring and alerting configuration."""
    health_check_interval_minutes: int = 5
    track_memory_usage: bool = True
    track_execution_time: bool = True
    max_task_duration_minutes: int = 120
    max_memory_usage_mb: int = 4096
    min_success_rate_percent: float = 90.0


class Settings:
    """Main settings class that loads and manages all configuration."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize settings.
        
        Args:
            config_path: Path to YAML configuration file. If None, uses default path.
        """
        self.config_path = config_path or self._get_default_config_path()
        self.config_data: Dict[str, Any] = {}
        self.load_config()
        
        # Initialize configuration objects
        self.database = self._load_database_config()
        self.api = self._load_api_config()
        self.bronze_tokens = self._load_bronze_tokens_config()
        self.silver_tokens = self._load_silver_tokens_config()
        self.bronze_whales = self._load_bronze_whales_config()
        self.silver_whales = self._load_silver_whales_config()
        self.bronze_transactions = self._load_bronze_transactions_config()
        self.silver_wallet_pnl = self._load_silver_wallet_pnl_config()
        self.smart_traders = self._load_smart_traders_config()
        self.monitoring = self._load_monitoring_config()
    
    def _get_default_config_path(self) -> str:
        """Get default configuration file path."""
        # Try multiple possible locations
        possible_paths = [
            "/opt/airflow/config/pipeline_config.yaml",  # Docker path
            "airflow/config/pipeline_config.yaml",       # Relative path
            "../airflow/config/pipeline_config.yaml",    # From src directory
            os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "config", "pipeline_config.yaml")
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        # If no file found, use the first path as default
        return possible_paths[0]
    
    def load_config(self):
        """Load configuration from YAML file."""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as file:
                    self.config_data = yaml.safe_load(file) or {}
                logger.debug(f"Loaded configuration from {self.config_path}")
            else:
                logger.warning(f"Configuration file not found at {self.config_path}, using defaults")
                self.config_data = {}
        except Exception as e:
            logger.error(f"Failed to load configuration from {self.config_path}: {e}")
            self.config_data = {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key.
        
        Args:
            key: Configuration key (supports dot notation like 'database.pool_size')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        # Support environment variable override
        env_key = key.upper().replace('.', '_')
        env_value = os.getenv(env_key)
        if env_value is not None:
            # Try to convert to appropriate type
            if isinstance(default, bool):
                return env_value.lower() in ('true', '1', 'yes', 'on')
            elif isinstance(default, int):
                try:
                    return int(env_value)
                except ValueError:
                    pass
            elif isinstance(default, float):
                try:
                    return float(env_value)
                except ValueError:
                    pass
            return env_value
        
        # Navigate nested dictionary using dot notation
        keys = key.split('.')
        value = self.config_data
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def _load_database_config(self) -> DatabaseConfig:
        """Load database configuration."""
        db_config = self.config_data.get('database', {})
        return DatabaseConfig(
            pool_size=db_config.get('pool_size', 10),
            max_overflow=db_config.get('max_overflow', 20),
            pool_recycle_hours=db_config.get('pool_recycle_hours', 1),
            connection_timeout_seconds=db_config.get('connection_timeout_seconds', 30),
            default_batch_size=db_config.get('default_batch_size', 1000),
            max_batch_size=db_config.get('max_batch_size', 5000)
        )
    
    def _load_api_config(self) -> APIConfig:
        """Load API configuration."""
        api_config = self.config_data.get('api', {})
        birdeye_config = api_config.get('birdeye', {})
        
        return APIConfig(
            birdeye_base_url=birdeye_config.get('base_url', "https://public-api.birdeye.so"),
            rate_limit_per_second=birdeye_config.get('rate_limit_per_second', 2),
            timeout_seconds=birdeye_config.get('timeout_seconds', 30),
            max_retries=birdeye_config.get('max_retries', 3),
            backoff_factor=birdeye_config.get('backoff_factor', 2)
        )
    
    def _load_bronze_tokens_config(self) -> BronzeTokensConfig:
        """Load bronze tokens configuration."""
        config = self.config_data.get('bronze_tokens', {})
        return BronzeTokensConfig(
            token_limit=config.get('token_limit', 50),
            min_liquidity=config.get('min_liquidity', 100000),
            max_liquidity=config.get('max_liquidity', 50000000),
            min_volume_1h_usd=config.get('min_volume_1h_usd', 50000),
            min_price_change_2h_percent=config.get('min_price_change_2h_percent', 15.0),
            min_price_change_24h_percent=config.get('min_price_change_24h_percent', 20.0),
            api_pagination_limit=config.get('api_pagination_limit', 50),
            api_rate_limit_delay=config.get('api_rate_limit_delay', 0.5),
            batch_size=config.get('batch_size', 10),
            max_processing_time_minutes=config.get('max_processing_time_minutes', 30)
        )
    
    def _load_silver_tokens_config(self) -> SilverTokensConfig:
        """Load silver tokens configuration."""
        config = self.config_data.get('silver_tokens', {})
        return SilverTokensConfig(
            min_holders=config.get('min_holders', 100),
            min_volume_24h=config.get('min_volume_24h', 50000),
            selection_ratio=config.get('selection_ratio', 0.2),
            max_tokens_selected=config.get('max_tokens_selected', 20),
            score_weights=config.get('score_weights', {
                "liquidity": 0.3,
                "volume_24h": 0.25,
                "price_change_24h": 0.2,
                "holder_count": 0.15,
                "trade_count_24h": 0.1
            })
        )
    
    def _load_bronze_whales_config(self) -> BronzeWhalesConfig:
        """Load bronze whales configuration."""
        config = self.config_data.get('bronze_whales', {})
        return BronzeWhalesConfig(
            top_holders_limit=config.get('top_holders_limit', 20),
            min_holding_percentage=config.get('min_holding_percentage', 0.5),
            api_rate_limit_delay=config.get('api_rate_limit_delay', 0.5),
            max_tokens_per_run=config.get('max_tokens_per_run', 10),
            batch_size=config.get('batch_size', 5),
            refetch_interval_hours=config.get('refetch_interval_hours', 72)
        )
    
    def _load_silver_whales_config(self) -> SilverWhalesConfig:
        """Load silver whales configuration."""
        config = self.config_data.get('silver_whales', {})
        return SilverWhalesConfig(
            min_tokens_held=config.get('min_tokens_held', 2),
            min_total_value_usd=config.get('min_total_value_usd', 10000),
            exclude_cex_addresses=config.get('exclude_cex_addresses', True),
            max_whales_selected=config.get('max_whales_selected', 100),
            excluded_addresses=config.get('excluded_addresses', [
                "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
                "7YttLkHDoNj9wyDur5pM1ejNaAvT9X4eqaYcHQqtj2G5",
                "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
            ])
        )
    
    def _load_bronze_transactions_config(self) -> BronzeTransactionsConfig:
        """Load bronze transactions configuration."""
        config = self.config_data.get('bronze_transactions', {})
        return BronzeTransactionsConfig(
            max_transactions_per_wallet=config.get('max_transactions_per_wallet', 100),
            lookback_days=config.get('lookback_days', 30),
            wallet_api_delay=config.get('wallet_api_delay', 1.0),
            batch_size=config.get('batch_size', 5),
            max_wallets_per_run=config.get('max_wallets_per_run', 20),
            refetch_interval_days=config.get('refetch_interval_days', 30),
            min_transaction_value_usd=config.get('min_transaction_value_usd', 100),
            include_transaction_types=config.get('include_transaction_types', ["swap", "transfer"])
        )
    
    def _load_silver_wallet_pnl_config(self) -> SilverWalletPnLConfig:
        """Load silver wallet PnL configuration."""
        config = self.config_data.get('silver_wallet_pnl', {})
        return SilverWalletPnLConfig(
            calculation_method=config.get('calculation_method', "fifo"),
            include_unrealized=config.get('include_unrealized', False),
            min_trades_for_calculation=config.get('min_trades_for_calculation', 5),
            wallet_batch_size=config.get('wallet_batch_size', 10),
            max_processing_time_minutes=config.get('max_processing_time_minutes', 60),
            memory_limit_mb=config.get('memory_limit_mb', 2048),
            min_realized_pnl_usd=config.get('min_realized_pnl_usd', 100),
            min_win_rate_percent=config.get('min_win_rate_percent', 30)
        )
    
    def _load_smart_traders_config(self) -> SmartTradersConfig:
        """Load smart traders configuration."""
        config = self.config_data.get('smart_traders', {})
        return SmartTradersConfig(
            min_win_rate=config.get('min_win_rate', 60.0),
            min_realized_pnl=config.get('min_realized_pnl', 1000.0),
            min_trade_count=config.get('min_trade_count', 10),
            min_trading_days=config.get('min_trading_days', 7),
            performance_tiers=config.get('performance_tiers', {
                "elite": {"min_pnl": 10000, "min_roi": 50, "min_win_rate": 70},
                "strong": {"min_pnl": 5000, "min_roi": 25, "min_win_rate": 60},
                "promising": {"min_pnl": 1000, "min_roi": 15, "min_win_rate": 50},
                "qualified": {"min_pnl": 100, "min_roi": 5, "min_win_rate": 40}
            }),
            scoring_weights=config.get('scoring_weights', {
                "realized_pnl": 0.4,
                "roi_percentage": 0.3,
                "win_rate": 0.2,
                "trade_frequency": 0.1
            })
        )
    
    def _load_monitoring_config(self) -> MonitoringConfig:
        """Load monitoring configuration."""
        config = self.config_data.get('monitoring', {})
        alerts = config.get('alerts', {})
        
        return MonitoringConfig(
            health_check_interval_minutes=config.get('health_check_interval_minutes', 5),
            track_memory_usage=config.get('track_memory_usage', True),
            track_execution_time=config.get('track_execution_time', True),
            max_task_duration_minutes=alerts.get('max_task_duration_minutes', 120),
            max_memory_usage_mb=alerts.get('max_memory_usage_mb', 4096),
            min_success_rate_percent=alerts.get('min_success_rate_percent', 90.0)
        )
    
    # Environment-specific getters
    
    def get_database_url(self) -> str:
        """Get database URL from environment variables."""
        user = os.getenv('POSTGRES_USER', 'trader')
        password = os.getenv('POSTGRES_PASSWORD', 'trader_password')
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'solana-smart-traders')
        
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    
    def get_birdeye_api_key(self) -> str:
        """Get BirdEye API key from Airflow Variables or environment variables."""
        # First try Airflow Variables (preferred in Airflow environment)
        try:
            from airflow.models import Variable
            api_key = Variable.get("BIRDEYE_API_KEY", default_var=None)
            if api_key:
                return api_key
        except ImportError:
            # Not in Airflow environment, fall back to environment variables
            pass
        except Exception:
            # Variable not found or other Airflow error, fall back to environment variables
            pass
        
        # Fall back to environment variables
        api_key = os.getenv('BIRDEYE_API_KEY')
        if not api_key:
            raise ValueError("BIRDEYE_API_KEY must be set in Airflow Variables or environment variables")
        return api_key
    
    def is_development(self) -> bool:
        """Check if running in development mode."""
        env = os.getenv('ENVIRONMENT', 'production').lower()
        return env in ('development', 'dev', 'local')
    
    def is_test_mode(self) -> bool:
        """Check if running in test mode."""
        return self.get('development.test_mode', False) or os.getenv('TEST_MODE', '').lower() == 'true'
    
    @property
    def birdeye_api_key(self) -> str:
        """Get BirdEye API key as a property."""
        return self.get_birdeye_api_key()


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the global settings instance.
    
    Returns:
        Settings instance
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings():
    """Reload settings from configuration file."""
    global _settings
    _settings = Settings()
    logger.info("Settings reloaded")


# Convenience functions for common configuration access

def get_database_config() -> DatabaseConfig:
    """Get database configuration."""
    return get_settings().database


def get_api_config() -> APIConfig:
    """Get API configuration."""
    return get_settings().api


def get_bronze_tokens_config() -> BronzeTokensConfig:
    """Get bronze tokens configuration."""
    return get_settings().bronze_tokens


def get_silver_tokens_config() -> SilverTokensConfig:
    """Get silver tokens configuration."""
    return get_settings().silver_tokens


def get_bronze_whales_config() -> BronzeWhalesConfig:
    """Get bronze whales configuration."""
    return get_settings().bronze_whales


def get_silver_whales_config() -> SilverWhalesConfig:
    """Get silver whales configuration."""
    return get_settings().silver_whales


def get_bronze_transactions_config() -> BronzeTransactionsConfig:
    """Get bronze transactions configuration."""
    return get_settings().bronze_transactions


def get_silver_wallet_pnl_config() -> SilverWalletPnLConfig:
    """Get silver wallet PnL configuration."""
    return get_settings().silver_wallet_pnl


def get_smart_traders_config() -> SmartTradersConfig:
    """Get smart traders configuration."""
    return get_settings().smart_traders


def get_monitoring_config() -> MonitoringConfig:
    """Get monitoring configuration."""
    return get_settings().monitoring