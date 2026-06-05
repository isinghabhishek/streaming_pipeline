import sys
import os

# Add project root to sys.path so test modules can import project packages
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
