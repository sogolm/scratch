# -*- coding: utf-8 -*-

# Scrapy settings for tutorial project

BOT_NAME = 'tutorial'

SPIDER_MODULES = ['tutorial.spiders']
NEWSPIDER_MODULE = 'tutorial.spiders'
ITEM_PIPELINES = { 'tutorial.pipelines.JsonWriterPipeline':300 }

