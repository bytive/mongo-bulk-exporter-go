# 🚀 MongoDB Bulk Exporter

A high-performance **MongoDB Exporter** built in Go to efficiently export **100M+ records** in parallel with automatic resumption and batch processing.

## 🔹 Features
✅ **Fast & Efficient** – Exports large datasets in parallel  
✅ **Automatic Resumption** – Continues from last `_id` if interrupted  
✅ **Batch Processing** – Saves records in JSON batches  
✅ **Minimal Resource Usage** – Uses `_id` indexing for fast pagination  
✅ **Configurable Workers** – Optimized parallel processing  

## 📦 Installation
1. **Clone the repository**
   ```sh
   git clone https://github.com/yourusername/mongo-bulk-exporter.git
   cd mongo-bulk-exporter
