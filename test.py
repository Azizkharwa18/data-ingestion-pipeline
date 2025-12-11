from celery_app import generate_report_task
import pprint

print("Requesting secure report...")
task = generate_report_task.delay("large_sales_data.csv")
result = task.get()

print("\n--- What the Network/Database Sees ---")
pprint.pprint(result) 
# You will see a giant block of random text: "-----BEGIN PGP MESSAGE-----..."