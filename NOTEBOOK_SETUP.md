# 🎯 Solana Smart Traders - Jupyter Notebook Setup Complete!

## 🚀 Quick Access

### **Access Your Jupyter Notebook:**
- **URL**: http://localhost:8888
- **Token**: `solana-smart-traders`
- **Interface**: JupyterLab (modern interface)

### **Available Notebooks:**
1. **`quick_start.ipynb`** - Start here for basic database connectivity test
2. **`database_explorer.ipynb`** - Comprehensive data exploration with pre-built queries

## 📊 What You Can Explore

### **Bronze Layer (Raw Data)**
- `bronze.bronze_tokens` - Token data from BirdEye API
- `bronze.bronze_whales` - Top token holders  
- `bronze.bronze_transactions` - Transaction history

### **Silver Layer (Processed Data)**
- `silver.silver_tokens` - Selected tokens for tracking
- `silver.silver_whales` - Unique whale addresses
- `silver.silver_wallet_pnl` - Trading performance calculations

### **Gold Layer (Analytics)**
- `gold.smart_traders` - Final ranked smart traders

### **Pipeline Layer (Monitoring)**
- `pipeline.entity_processing_state` - State tracking
- `pipeline.task_execution_log` - Execution history

## 🔧 Management Commands

### **Start Notebook**
```bash
./start_notebook.sh
```

### **Stop Notebook**
```bash
docker compose -f docker-compose.notebook.yml down
```

### **Restart Notebook**
```bash
docker compose -f docker-compose.notebook.yml restart
```

### **View Logs**
```bash
docker logs solana-jupyter
```

## 📈 Current Data Status

Based on your pipeline execution:
- ✅ **4 Smart Traders** identified and ranked
- ✅ **3 PROMISING** tier traders  
- ✅ **1 STRONG** tier trader
- ✅ **389 unique whale addresses** processed
- ✅ **State tracking** fully implemented and working
- ✅ **All database layers** populated with data

## 🎛️ Pre-Built Analysis Features

### **Smart Trader Analysis**
- Performance tier distribution
- Trading metrics breakdown
- Daily PnL and trade averages
- Portfolio characteristics

### **State Tracking Validation**
- Processing status across all layers
- Data flow integrity checks
- Record count validation

### **Token Performance**
- Market cap analysis
- Whale activity correlation
- Volume and liquidity metrics

### **Custom Query Support**
- SQL query execution
- Data export capabilities
- Visualization examples

## 🔍 Sample Queries You Can Run

### **Top Smart Traders**
```sql
SELECT 
    performance_tier,
    wallet_address,
    total_realized_pnl_usd,
    win_rate_percent,
    total_trades
FROM gold.smart_traders
ORDER BY total_realized_pnl_usd DESC;
```

### **Pipeline Status**
```sql
SELECT 
    'bronze_tokens' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN silver_processed = true THEN 1 END) as processed_records
FROM bronze.bronze_tokens;
```

### **Whale Activity**
```sql
SELECT 
    token_symbol,
    COUNT(DISTINCT wallet_address) as unique_whales,
    AVG(ui_amount) as avg_holding
FROM bronze.bronze_whales
GROUP BY token_symbol
ORDER BY unique_whales DESC;
```

## 🛠️ Technical Details

### **Database Connection**
- **Host**: `solana_postgres` (within Docker network)
- **Database**: `solana-smart-traders`
- **Username**: `trader`
- **Password**: `trader_password`
- **Note**: The notebook will automatically try multiple connection options to find the correct hostname

### **Container Details**
- **Container**: `solana-jupyter`
- **Image**: `jupyter/scipy-notebook:latest`
- **Network**: `solana_smart_traders_network`

### **Installed Packages**
- `pandas` - Data manipulation
- `sqlalchemy` - Database connectivity
- `psycopg2-binary` - PostgreSQL adapter
- `matplotlib` - Plotting
- `seaborn` - Statistical visualization
- `plotly` - Interactive charts

## 🎨 Next Steps

1. **Start with `quick_start.ipynb`** to verify everything works
2. **Explore `database_explorer.ipynb`** for comprehensive analysis
3. **Create custom queries** for specific insights
4. **Visualize data** using the built-in plotting libraries

## 📚 Documentation

- **Notebook README**: `notebooks/README.md`
- **Project README**: `README.md`
- **Configuration**: `CLAUDE.md`

## 🚨 Troubleshooting

### **Connection Issues**
- Ensure PostgreSQL is running: `docker compose ps`
- Check network connectivity: `docker network ls`
- Verify database access: `docker compose exec postgres psql -U trader -d solana-smart-traders -c "SELECT 1;"`

### **Performance Issues**
- Use `LIMIT` in queries for large datasets
- Restart notebook kernel if memory issues occur
- Check container logs: `docker logs solana-jupyter`

---

**🎉 You're all set! Open http://localhost:8888 and start exploring your Solana Smart Traders data!**