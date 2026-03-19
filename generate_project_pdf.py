"""
Generate SPX Data Pipeline Project Documentation PDF
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, cm
from reportlab.lib.colors import HexColor, black, white
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, ListFlowable, ListItem, KeepTogether
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from datetime import datetime

# Colors
PRIMARY_BLUE = HexColor('#1a365d')
SECONDARY_BLUE = HexColor('#2b6cb0')
LIGHT_BLUE = HexColor('#ebf8ff')
ACCENT_GREEN = HexColor('#38a169')
LIGHT_GREEN = HexColor('#f0fff4')
GRAY = HexColor('#718096')

def create_styles():
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name='CustomTitle',
        parent=styles['Title'],
        fontSize=28,
        textColor=PRIMARY_BLUE,
        spaceAfter=30,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    ))

    styles.add(ParagraphStyle(
        name='SubTitle',
        parent=styles['Normal'],
        fontSize=14,
        textColor=SECONDARY_BLUE,
        spaceAfter=20,
        alignment=TA_CENTER,
        fontName='Helvetica'
    ))

    styles.add(ParagraphStyle(
        name='SectionHeading',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=PRIMARY_BLUE,
        spaceBefore=20,
        spaceAfter=12,
        fontName='Helvetica-Bold'
    ))

    styles.add(ParagraphStyle(
        name='SubHeading',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=SECONDARY_BLUE,
        spaceBefore=15,
        spaceAfter=8,
        fontName='Helvetica-Bold'
    ))

    styles.add(ParagraphStyle(
        name='SubSubHeading',
        parent=styles['Heading3'],
        fontSize=12,
        textColor=PRIMARY_BLUE,
        spaceBefore=10,
        spaceAfter=6,
        fontName='Helvetica-Bold'
    ))

    styles.add(ParagraphStyle(
        name='CustomBody',
        parent=styles['Normal'],
        fontSize=10,
        textColor=black,
        spaceBefore=6,
        spaceAfter=6,
        alignment=TA_JUSTIFY,
        fontName='Helvetica',
        leading=14
    ))

    styles.add(ParagraphStyle(
        name='BulletText',
        parent=styles['Normal'],
        fontSize=10,
        textColor=black,
        spaceBefore=3,
        spaceAfter=3,
        leftIndent=20,
        fontName='Helvetica',
        leading=14
    ))

    styles.add(ParagraphStyle(
        name='CodeBlock',
        parent=styles['Normal'],
        fontSize=8,
        textColor=black,
        backColor=LIGHT_BLUE,
        spaceBefore=6,
        spaceAfter=6,
        leftIndent=15,
        fontName='Courier',
        leading=12
    ))

    styles.add(ParagraphStyle(
        name='TableHeader',
        parent=styles['Normal'],
        fontSize=10,
        textColor=white,
        fontName='Helvetica-Bold',
        alignment=TA_CENTER
    ))

    styles.add(ParagraphStyle(
        name='TableCell',
        parent=styles['Normal'],
        fontSize=9,
        textColor=black,
        fontName='Helvetica',
        alignment=TA_LEFT
    ))

    styles.add(ParagraphStyle(
        name='Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=GRAY,
        alignment=TA_CENTER
    ))

    return styles

def create_document():
    doc = SimpleDocTemplate(
        "SPX_Data_Pipeline_Project.pdf",
        pagesize=A4,
        rightMargin=2*cm,
        leftMargin=2*cm,
        topMargin=2*cm,
        bottomMargin=2*cm
    )
    return doc

def build_story(styles):
    story = []

    # ========== Title Page ==========
    story.append(Spacer(1, 2*inch))
    story.append(Paragraph("SPX 500 Data Pipeline", styles['CustomTitle']))
    story.append(Paragraph("Medallion Architecture Implementation", styles['SubTitle']))
    story.append(Spacer(1, 0.5*inch))
    story.append(Paragraph("NUS MQF QF5214 Data Engineering Course Project", styles['SubTitle']))
    story.append(Spacer(1, 1*inch))

    # Project info table
    info_data = [
        ['Project', 'SPX 500 Data Pipeline'],
        ['Course', 'QF5214 Data Engineering'],
        ['Architecture', 'Medallion (Bronze / Silver / Gold)'],
        ['Status', 'Phase 1 & 2 Completed'],
        ['Date', datetime.now().strftime('%Y-%m-%d')],
    ]
    info_table = Table(info_data, colWidths=[2.5*inch, 3.5*inch])
    info_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), LIGHT_BLUE),
        ('TEXTCOLOR', (0, 0), (-1, -1), PRIMARY_BLUE),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
        ('ALIGN', (1, 0), (1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, SECONDARY_BLUE),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    story.append(info_table)

    story.append(PageBreak())

    # ========== Table of Contents ==========
    story.append(Paragraph("Table of Contents", styles['SectionHeading']))
    story.append(Spacer(1, 0.3*inch))

    toc_items = [
        "1. Project Overview",
        "2. Architecture Design",
        "3. Data Architecture",
        "4. Core Components",
        "5. DataProvider API",
        "6. Bronze Layer (Ingestion Engine)",
        "7. Implementation Phases",
        "8. Current Progress",
        "9. Next Steps",
    ]

    for item in toc_items:
        story.append(Paragraph(item, styles['CustomBody']))
        story.append(Spacer(1, 0.1*inch))

    story.append(PageBreak())

    # ========== Section 1: Project Overview ==========
    story.append(Paragraph("1. Project Overview", styles['SectionHeading']))

    story.append(Paragraph(
        "This project implements a production-like SPX 500 data pipeline using the Medallion architecture "
        "(Bronze, Silver, Gold layers). The system simulates a financial data API behavior, ingesting 20 years "
        "of OHLCV price data, fundamental financial data, and earnings call transcripts for approximately 818 "
        "SPX tickers.",
        styles['CustomBody']
    ))

    story.append(Paragraph("Project Goals", styles['SubHeading']))
    goals = [
        "Build a complete data engineering pipeline from raw data to analytics-ready format",
        "Implement Medallion architecture pattern (Bronze/Silver/Gold)",
        "Simulate real-world financial API behavior with DataProvider class",
        "Create robust ingestion engine with watchdog monitoring",
        "Enable sentiment analysis on earnings call transcripts",
        "Develop OLAP views for advanced analytics"
    ]
    for goal in goals:
        story.append(Paragraph(f"  - {goal}", styles['BulletText']))

    story.append(Paragraph("Data Scale", styles['SubHeading']))
    data_scale = [
        ['Data Type', 'Path', 'Scale'],
        ['Price (OHLCV)', 'data/price/spx_20yr_ohlcv_data.csv', '818 tickers, 5284 trading days'],
        ['Fundamentals', 'data/fundamental/SPX_Fundamental_History/', '5726 files, annual + quarterly'],
        ['PDF Transcripts', 'data/transcript/SPX_20yr_PDF_Library_10GB/', '32036 files'],
        ['Tickers', 'data/reference/tickers.csv', '947 entries'],
    ]
    scale_table = Table(data_scale, colWidths=[1.5*inch, 2.5*inch, 2*inch])
    scale_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), PRIMARY_BLUE),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, GRAY),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('BACKGROUND', (0, 1), (-1, -1), LIGHT_BLUE),
    ]))
    story.append(scale_table)

    story.append(PageBreak())

    # ========== Section 2: Architecture Design ==========
    story.append(Paragraph("2. Architecture Design", styles['SectionHeading']))

    story.append(Paragraph(
        "The system follows the Medallion Architecture pattern, which separates data processing into three stages:",
        styles['CustomBody']
    ))

    story.append(Paragraph("Medallion Layers", styles['SubHeading']))

    layers = [
        ['Layer', 'Description', 'Technology'],
        ['Bronze', 'Raw data ingestion from landing zone\nWatchdog monitoring', 'DuckDB, watchdog'],
        ['Silver', 'Clean data with sentiment analysis\nPartitioned Parquet storage', 'DuckDB SQL, TextBlob'],
        ['Gold', 'OLAP views for analytics\nBusiness intelligence', 'DuckDB views, Streamlit'],
    ]
    layers_table = Table(layers, colWidths=[1*inch, 3*inch, 2*inch])
    layers_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), PRIMARY_BLUE),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('GRID', (0, 0), (-1, -1), 0.5, GRAY),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (0, 1), (0, 1), HexColor('#2b6cb0')),  # Bronze
        ('BACKGROUND', (0, 2), (0, 2), HexColor('#38a169')),  # Silver
        ('BACKGROUND', (0, 3), (0, 3), HexColor('#d69e2e')),  # Gold
        ('TEXTCOLOR', (0, 1), (0, -1), white),
    ]))
    story.append(layers_table)
    story.append(Spacer(1, 0.3*inch))

    story.append(Paragraph("Data Flow Architecture", styles['SubHeading']))
    flow_text = """
    Existing Dataset (CSV/PDF) -> DataProvider API -> Bronze Layer (OLTP)
    -> ELT Pipeline -> Silver Layer (clean Parquet + sentiment)
    -> Gold Layer (OLAP views + Streamlit)
    """
    story.append(Paragraph(f"<pre>{flow_text}</pre>", styles['CodeBlock']))

    story.append(PageBreak())

    # ========== Section 3: Data Architecture ==========
    story.append(Paragraph("3. Data Architecture", styles['SectionHeading']))

    story.append(Paragraph("Landing Zone Structure", styles['SubHeading']))
    landing_text = """
    output/landing_zone/
    |-- prices/
    |   +-- price_YYYY-MM-DD.csv          # Daily price batch
    |-- fundamentals/
    |   +-- YYYY-MM-DD/
    |       +-- TICKER_type_freq.csv      # Financial report
    +-- transcripts/
        +-- TICKER_YYYY-MM-DD.pdf         # Earnings transcript PDF
    """
    story.append(Paragraph(f"<pre>{landing_text}</pre>", styles['CodeBlock']))

    story.append(Paragraph("Silver Layer Structure", styles['SubHeading']))
    silver_text = """
    silver/
    |-- price/
    |   +-- date=2004-01-02/data.parquet
    |-- fundamentals/
    |   +-- ticker=AAPL/data.parquet
    |-- transcript_text/
    |   +-- ticker=AAPL/date=2024-02-01/content.txt
    +-- transcript_sentiment/
        +-- ticker=AAPL/date=2024-02-01/sentiment.parquet
    """
    story.append(Paragraph(f"<pre>{silver_text}</pre>", styles['CodeBlock']))

    story.append(PageBreak())

    # ========== Section 4: Core Components ==========
    story.append(Paragraph("4. Core Components", styles['SectionHeading']))

    components = [
        ['Component', 'File', 'Responsibility'],
        ['DataProvider API', 'pipeline/data_provider.py', 'Simulates Yahoo Finance API,\nread from CSV/PDF files'],
        ['Simulator', 'pipeline/simulators/comprehensive_simulator.py', 'Virtual clock, generates data\nto landing zone'],
        ['Ingestion Engine', 'pipeline/ingestion_engine.py', 'Watchdog monitoring, ingests\nfiles to Bronze tables'],
        ['ELT Pipeline', 'pipeline/elt_pipeline.py', 'Bronze to Silver transform,\nsentiment analysis'],
        ['OLAP Views', 'duckdb/spx_analytics.duckdb', 'Sentiment-price correlation,\nsector statistics'],
        ['Dashboard', 'pipeline/dashboard.py', 'Streamlit real-time monitoring'],
    ]
    comp_table = Table(components, colWidths=[1.5*inch, 2*inch, 2.5*inch])
    comp_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), PRIMARY_BLUE),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('GRID', (0, 0), (-1, -1), 0.5, GRAY),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('BACKGROUND', (0, 1), (-1, -1), LIGHT_BLUE),
    ]))
    story.append(comp_table)

    story.append(PageBreak())

    # ========== Section 5: DataProvider API ==========
    story.append(Paragraph("5. DataProvider API", styles['SectionHeading']))

    story.append(Paragraph(
        "The SPXDataProvider class encapsulates raw data access and simulates Yahoo Finance API behavior. "
        "All data access must go through this class.",
        styles['CustomBody']
    ))

    story.append(Paragraph("API Methods", styles['SubHeading']))
    api_methods = [
        ['Method', 'Returns', 'Description'],
        ['get_price(ticker, date)', 'DataFrame', 'OHLCV price data for specific date'],
        ['get_fundamentals(ticker, freq)', 'dict', 'Fundamental data (income, balance, cashflow)'],
        ['get_transcript(ticker, date)', 'bytes', 'Raw PDF bytes of earnings transcript'],
        ['list_transcripts(ticker, year)', 'list', 'List available transcripts, filtered'],
        ['get_trading_dates(start, end)', 'list', 'Trading dates in specified range'],
        ['get_ticker_list()', 'list', 'All available ticker symbols'],
    ]
    api_table = Table(api_methods, colWidths=[2.5*inch, 1*inch, 2.5*inch])
    api_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), SECONDARY_BLUE),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, GRAY),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('BACKGROUND', (0, 1), (-1, -1), white),
    ]))
    story.append(api_table)

    story.append(Paragraph("Error Handling", styles['SubHeading']))
    errors = [
        "Data exists -> DataFrame/dict/bytes returned",
        "No data -> empty DataFrame/empty dict/FileNotFoundError",
        "Ticker not found -> ValueError",
        "Corrupted file -> DataIntegrityError",
    ]
    for err in errors:
        story.append(Paragraph(f"  - {err}", styles['BulletText']))

    story.append(PageBreak())

    # ========== Section 6: Bronze Layer ==========
    story.append(Paragraph("6. Bronze Layer (Ingestion Engine)", styles['SectionHeading']))

    story.append(Paragraph(
        "The Bronze layer uses watchdog monitoring to detect new files in the landing zone "
        "and ingest them into DuckDB tables. It supports both continuous watch mode and "
        "one-time scan mode for backfill.",
        styles['CustomBody']
    ))

    story.append(Paragraph("Bronze Tables", styles['SubHeading']))
    bronze_tables = [
        ['Table', 'Description'],
        ['ingestion_audit', 'Tracks all ingestion events, errors, and status'],
        ['raw_price_stream', 'Ingested OHLCV price data with UNIQUE(ticker, date)'],
        ['raw_fundamental_index', 'Index of fundamental report files with fiscal dates'],
        ['raw_transcript_index', 'Index of transcript PDFs with event dates'],
    ]
    bronze_table = Table(bronze_tables, colWidths=[2*inch, 4*inch])
    bronze_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), PRIMARY_BLUE),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, GRAY),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('BACKGROUND', (0, 1), (-1, -1), LIGHT_BLUE),
    ]))
    story.append(bronze_table)

    story.append(Paragraph("Running the Ingestion Engine", styles['SubHeading']))
    run_text = """
    # Watch mode (continuous monitoring)
    python pipeline/ingestion_engine.py --mode watch

    # Scan mode (one-time backfill)
    python pipeline/ingestion_engine.py --mode scan
    """
    story.append(Paragraph(f"<pre>{run_text}</pre>", styles['CodeBlock']))

    story.append(PageBreak())

    # ========== Section 7: Implementation Phases ==========
    story.append(Paragraph("7. Implementation Phases", styles['SectionHeading']))

    phases = [
        ['Phase', 'Task', 'Duration', 'Status'],
        ['1', 'DataProvider API', '1-2 days', 'COMPLETED'],
        ['2', 'Bronze Layer (Ingestion Engine)', '2-3 days', 'COMPLETED'],
        ['3', 'ELT Pipeline (Transform Jobs)', '2-3 days', 'PENDING'],
        ['4', 'Silver Layer (Parquet + Sentiment)', '2-3 days', 'PENDING'],
        ['5', 'Gold Layer (OLAP Views)', '2-3 days', 'PENDING'],
        ['6', 'Streamlit Dashboard', '1-2 days', 'PENDING'],
    ]
    phases_table = Table(phases, colWidths=[0.6*inch, 2.5*inch, 1*inch, 1*inch])
    phases_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), PRIMARY_BLUE),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, GRAY),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('BACKGROUND', (0, 1), (-1, 2), LIGHT_GREEN),  # Completed phases
        ('BACKGROUND', (0, 3), (-1, -1), white),  # Pending phases
        ('TEXTCOLOR', (3, 1), (3, 2), ACCENT_GREEN),  # Completed status
        ('FONTNAME', (3, 1), (3, 2), 'Helvetica-Bold'),
    ]))
    story.append(phases_table)

    story.append(PageBreak())

    # ========== Section 8: Current Progress ==========
    story.append(Paragraph("8. Current Progress", styles['SectionHeading']))

    story.append(Paragraph("Phase 1: DataProvider API - COMPLETED", styles['SubHeading']))
    phase1_items = [
        "Created SPXDataProvider class in pipeline/data_provider.py",
        "Implemented all API methods: get_price, get_fundamentals, get_transcript, list_transcripts, get_trading_dates, get_ticker_list",
        "Handles NaN values, missing data, and error cases properly",
        "Verified with test integration: 947 tickers, AAPL data accessible",
    ]
    for item in phase1_items:
        story.append(Paragraph(f"  + {item}", styles['BulletText']))

    story.append(Paragraph("Phase 2: Bronze Layer - COMPLETED", styles['SubHeading']))
    phase2_items = [
        "Created DuckDB Bronze tables: ingestion_audit, raw_price_stream, raw_fundamental_index, raw_transcript_index",
        "Implemented ComprehensiveSimulator with virtual clock advancing",
        "Implemented IngestionEngine with watchdog monitoring and scan mode",
        "Verified full pipeline: Simulator -> Landing Zone -> Ingestion Engine -> Bronze Tables",
        "Tested with 3-day backfill: 2454 price records, 3 transcript records ingested successfully",
    ]
    for item in phase2_items:
        story.append(Paragraph(f"  + {item}", styles['BulletText']))

    story.append(Paragraph("Integration Test Results", styles['SubHeading']))
    test_results = [
        ['Component', 'Result', 'Details'],
        ['DataProvider.get_ticker_list()', 'PASS', '947 tickers available'],
        ['DataProvider.get_price()', 'PASS', 'AAPL 2024-01-17: 1 row returned'],
        ['DataProvider.get_fundamentals()', 'PASS', 'income, balance, cashflow keys returned'],
        ['DataProvider.list_transcripts()', 'PASS', 'AAPL 2024: 4 transcripts'],
        ['Simulator backfill', 'PASS', '3 days: 2454 price records emitted'],
        ['Ingestion Engine scan', 'PASS', '2457 total records ingested to Bronze'],
    ]
    test_table = Table(test_results, colWidths=[2*inch, 0.8*inch, 3.2*inch])
    test_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), SECONDARY_BLUE),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, GRAY),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('BACKGROUND', (1, 1), (1, -1), LIGHT_GREEN),
        ('TEXTCOLOR', (1, 1), (1, -1), ACCENT_GREEN),
        ('FONTNAME', (1, 1), (1, -1), 'Helvetica-Bold'),
    ]))
    story.append(test_table)

    story.append(PageBreak())

    # ========== Section 9: Next Steps ==========
    story.append(Paragraph("9. Next Steps", styles['SectionHeading']))

    story.append(Paragraph("Phase 3: ELT Pipeline", styles['SubHeading']))
    phase3_items = [
        "Implement Bronze to Silver transform logic",
        "Create Silver layer Parquet files partitioned by date/ticker",
        "Add data quality validation (price > 0, high >= low, etc.)",
        "Log quality issues to silver_quality_issues table",
    ]
    for item in phase3_items:
        story.append(Paragraph(f"  - {item}", styles['BulletText']))

    story.append(Paragraph("Phase 4: Silver Layer", styles['SubHeading']))
    phase4_items = [
        "Implement PDF text extraction for transcripts",
        "Integrate TextBlob for sentiment analysis",
        "Compute polarity (-1 to +1) and subjectivity scores",
        "Store sentiment in silver_transcript_sentiment",
    ]
    for item in phase4_items:
        story.append(Paragraph(f"  - {item}", styles['BulletText']))

    story.append(Paragraph("Phase 5: Gold Layer", styles['SubHeading']))
    phase5_items = [
        "Create v_sentiment_price_analysis view",
        "Create v_sector_stats view",
        "Implement sentiment-price correlation analysis",
        "Build multi-dimensional statistics by sector/year/quarter",
    ]
    for item in phase5_items:
        story.append(Paragraph(f"  - {item}", styles['BulletText']))

    story.append(Paragraph("Phase 6: Streamlit Dashboard", styles['SubHeading']))
    phase6_items = [
        "Real-time data counts per layer",
        "Sentiment-price correlation charts",
        "GICS sector heatmap",
        "Pipeline status indicator",
    ]
    for item in phase6_items:
        story.append(Paragraph(f"  - {item}", styles['BulletText']))

    story.append(Spacer(1, 0.5*inch))

    # ========== Footer ==========
    story.append(Paragraph(
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | NUS MQF QF5214 Data Engineering Project",
        styles['Footer']
    ))

    return story

def main():
    print("Generating SPX Data Pipeline Project PDF...")
    doc = create_document()
    styles = create_styles()
    story = build_story(styles)
    doc.build(story)
    print("PDF generated: SPX_Data_Pipeline_Project.pdf")

if __name__ == "__main__":
    main()
