"""Direct LLM client replacing LangChain dependencies."""
import os
import json
import time
import logging
from typing import Dict, Any, Optional
import streamlit as st
from datetime import datetime

logger = logging.getLogger(__name__)

class LLMClient:
    """Direct LLM client supporting OpenAI and Anthropic."""
    
    def __init__(self):
        """Initialize LLM client based on configuration."""
        self.model = self._get_config_value("LLM_MODEL")
        self.temperature = float(self._get_config_value("LLM_TEMPERATURE"))
        self.max_tokens = int(self._get_config_value("LLM_MAX_TOKENS", "4096"))
        self.top_p = float(self._get_config_value("LLM_TOP_P", "1"))
        
        # Determine provider and initialize client
        if "claude" in self.model.lower():
            self.provider = "anthropic"
            self._init_anthropic_client()
        else:
            self.provider = "openai" 
            self._init_openai_client()
            
        logger.info(f"Initialized {self.provider} client with model {self.model}")

    def _get_config_value(self, key: str, default: str = None) -> str:
        """Get configuration value from environment or Streamlit secrets."""
        try:
            return os.getenv(key) or st.secrets[key]
        except Exception:
            if default is not None:
                return default
            raise ValueError(f"Configuration {key} not found")

    def _init_openai_client(self):
        """Initialize OpenAI client."""
        try:
            import openai
            self.client = openai.OpenAI(
                api_key=self._get_config_value("OPENAI_API_KEY"),
                timeout=60.0  # Add explicit timeout
            )
        except ImportError:
            raise ImportError("OpenAI package not installed. Run: pip install openai")

    def _init_anthropic_client(self):
        """Initialize Anthropic client."""
        try:
            import anthropic
            self.client = anthropic.Anthropic(
                api_key=self._get_config_value("ANTHROPIC_API_KEY"),
                timeout=60.0  # Add explicit timeout
            )
        except ImportError:
            raise ImportError("Anthropic package not installed. Run: pip install anthropic")

    def generate(self, prompt: str, max_retries: int = 3) -> str:
        """Generate response with retry logic and better error handling."""
        start_time = time.time()
        
        for attempt in range(max_retries):
            try:
                if self.provider == "openai":
                    result = self._call_openai(prompt)
                else:
                    result = self._call_anthropic(prompt)
                
                # Log successful call
                duration = time.time() - start_time
                logger.info(f"{self.provider} API call succeeded in {duration:.2f}s (attempt {attempt + 1})")
                return result
                    
            except Exception as e:
                duration = time.time() - start_time
                error_type = type(e).__name__
                
                # Log the error with details
                logger.warning(f"{self.provider} API call failed after {duration:.2f}s (attempt {attempt + 1}/{max_retries}): {error_type} - {str(e)}")
                
                if attempt == max_retries - 1:
                    # On final failure, log and re-raise with context
                    logger.error(f"All {max_retries} {self.provider} API attempts failed. Final error: {str(e)}")
                    raise RuntimeError(f"LLM API failed after {max_retries} attempts: {str(e)}")
                
                # Exponential backoff with jitter
                sleep_time = (2 ** attempt) + (time.time() % 1)  # Add jitter
                logger.info(f"Retrying in {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)

    def _call_openai(self, prompt: str) -> str:
        """Make OpenAI API call with improved error handling."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                top_p=self.top_p
            )
            
            if not response.choices or not response.choices[0].message.content:
                raise ValueError("Empty response from OpenAI API")
                
            return response.choices[0].message.content
            
        except Exception as e:
            # Add context to OpenAI-specific errors
            if "rate_limit" in str(e).lower():
                raise RuntimeError(f"OpenAI rate limit exceeded: {str(e)}")
            elif "quota" in str(e).lower():
                raise RuntimeError(f"OpenAI quota exceeded: {str(e)}")
            else:
                raise

    def _call_anthropic(self, prompt: str) -> str:
        """Make Anthropic API call with improved error handling."""
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                top_p=self.top_p,
                messages=[{"role": "user", "content": prompt}]
            )
            
            if not response.content or not response.content[0].text:
                raise ValueError("Empty response from Anthropic API")
                
            return response.content[0].text
            
        except Exception as e:
            # Add context to Anthropic-specific errors
            if "rate_limit" in str(e).lower():
                raise RuntimeError(f"Anthropic rate limit exceeded: {str(e)}")
            elif "quota" in str(e).lower() or "credit" in str(e).lower():
                raise RuntimeError(f"Anthropic quota/credits exceeded: {str(e)}")
            else:
                raise

    def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics for monitoring."""
        return {
            "provider": self.provider,
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "initialized_at": datetime.now().isoformat()
        }