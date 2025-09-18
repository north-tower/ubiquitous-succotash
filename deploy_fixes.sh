#!/bin/bash

# Script to deploy CORS and error handling fixes to VPS
# Run this from your local machine

echo "🚀 Deploying CORS and error handling fixes to VPS..."

# Copy the updated files to your VPS
echo "📁 Copying updated router files..."
scp app/routers/transactions.py root@164.68.115.204:/path/to/your/app/routers/
scp app/routers/financial_institutions.py root@164.68.115.204:/path/to/your/app/routers/
scp app/routers/credit_score.py root@164.68.115.204:/path/to/your/app/routers/
scp app/routers/lifestyle.py root@164.68.115.204:/path/to/your/app/routers/
scp app/routers/utility.py root@164.68.115.204:/path/to/your/app/routers/
scp app/main.py root@164.68.115.204:/path/to/your/app/

echo "🔄 Restarting Docker containers on VPS..."
ssh root@164.68.115.204 "cd /path/to/your/project && docker-compose down && docker-compose up -d"

echo "✅ Deployment complete!"
echo "🔍 Check your application logs:"
echo "ssh root@164.68.115.204 'docker logs core_api'"
