#!/usr/bin/env python3
"""
Detailed script to check Kafka topics with full metadata
"""
from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
import sys

# Configuration
BOOTSTRAP_SERVERS = 'linyixundeMacBook-Air.local:8092'  # Update if needed

def check_topics():
    """Check and display all Kafka topics with detailed information"""
    
    try:
        print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
        
        # Create admin client
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            request_timeout_ms=10000
        )
        
        print("✓ Connected successfully!\n")
        
        # Get all topics
        topics = admin.list_topics()
        
        # Filter out internal topics (optional)
        internal_topics = [t for t in topics if t.startswith('__')]
        user_topics = [t for t in topics if not t.startswith('__')]
        
        print("=" * 70)
        print(f"KAFKA TOPICS SUMMARY")
        print("=" * 70)
        print(f"Total topics: {len(topics)}")
        print(f"  User topics: {len(user_topics)}")
        print(f"  Internal topics: {len(internal_topics)}")
        print("=" * 70)
        
        # Display user topics
        if user_topics:
            print(f"\n📋 USER TOPICS ({len(user_topics)}):")
            print("-" * 70)
            for topic in sorted(user_topics):
                print(f"  • {topic}")
        
        # Display internal topics
        if internal_topics:
            print(f"\n🔧 INTERNAL TOPICS ({len(internal_topics)}):")
            print("-" * 70)
            for topic in sorted(internal_topics):
                print(f"  • {topic}")
        
        # Get detailed metadata for user topics
        if user_topics:
            print(f"\n📊 DETAILED TOPIC INFORMATION:")
            print("=" * 70)
            
            topic_metadata = admin.describe_topics(user_topics)
            
            for topic_info in topic_metadata:
                topic_name = topic_info['topic']
                partitions = topic_info['partitions']
                
                print(f"\nTopic: {topic_name}")
                print(f"  Partitions: {len(partitions)}")
                print(f"  Details:")
                
                for partition in partitions:
                    print(f"    Partition {partition['partition']}:")
                    print(f"      Leader: {partition['leader']}")
                    print(f"      Replicas: {partition['replicas']}")
                    print(f"      ISR (In-Sync Replicas): {partition['isr']}")
        
        # Get topic configurations (optional)
        print(f"\n⚙️  TOPIC CONFIGURATIONS:")
        print("=" * 70)
        
        for topic in sorted(user_topics):
            try:
                config_resource = ConfigResource(
                    ConfigResourceType.TOPIC,
                    topic
                )
                configs = admin.describe_configs([config_resource])
                
                print(f"\nTopic: {topic}")
                for config in configs:
                    for value in config.resources[0][4]:
                        print(f"{value}")
                            
            except Exception as e:
                print(f"  Could not fetch config: {e}")
        
        admin.close()
        print("\n" + "=" * 70)
        print("✓ Topic check completed successfully!")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    check_topics()
