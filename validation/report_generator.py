"""
HTML report generation for validation results.

Generates comprehensive, visual HTML reports from validation results.
"""

from pathlib import Path
from typing import Dict, Any
from datetime import datetime
import json


def generate_html_report(results: Dict[str, Any], output_file: Path):
    """
    Generate an HTML report from validation results.

    Args:
        results: Validation results dictionary
        output_file: Path to save the HTML report
    """
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>xclim-timber Validation Report - {results.get('pipeline', 'Unknown')} Pipeline</title>
    <style>
        :root {{
            --color-pass: #28a745;
            --color-warning: #ffc107;
            --color-fail: #dc3545;
            --color-error: #721c24;
            --color-bg: #f8f9fa;
            --color-border: #dee2e6;
        }}

        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            background: var(--color-bg);
            padding: 20px;
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            padding: 30px;
        }}

        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid var(--color-border);
            padding-bottom: 15px;
            margin-bottom: 30px;
        }}

        h2 {{
            color: #34495e;
            margin-top: 30px;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid var(--color-border);
        }}

        h3 {{
            color: #495057;
            margin-top: 20px;
            margin-bottom: 15px;
        }}

        .status-badge {{
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-weight: bold;
            color: white;
            margin-left: 10px;
        }}

        .status-pass {{ background-color: var(--color-pass); }}
        .status-warning {{ background-color: var(--color-warning); color: #333; }}
        .status-fail {{ background-color: var(--color-fail); }}
        .status-error {{ background-color: var(--color-error); }}

        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}

        .summary-card {{
            background: var(--color-bg);
            border-radius: 8px;
            padding: 20px;
            text-align: center;
            border: 2px solid var(--color-border);
        }}

        .summary-card.pass {{ border-color: var(--color-pass); }}
        .summary-card.warning {{ border-color: var(--color-warning); }}
        .summary-card.fail {{ border-color: var(--color-fail); }}

        .summary-value {{
            font-size: 2em;
            font-weight: bold;
            margin: 10px 0;
        }}

        .summary-label {{
            color: #6c757d;
            font-size: 0.9em;
            text-transform: uppercase;
        }}

        .validation-section {{
            margin: 30px 0;
            padding: 20px;
            background: var(--color-bg);
            border-radius: 8px;
        }}

        .validation-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}

        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid var(--color-border);
        }}

        th {{
            background: var(--color-bg);
            font-weight: 600;
            color: #495057;
        }}

        tr:hover {{
            background: rgba(0,0,0,0.02);
        }}

        .error-list, .warning-list {{
            margin: 15px 0;
            padding: 15px;
            border-radius: 5px;
        }}

        .error-list {{
            background: #f8d7da;
            border: 1px solid #f5c6cb;
            color: #721c24;
        }}

        .warning-list {{
            background: #fff3cd;
            border: 1px solid #ffeeba;
            color: #856404;
        }}

        .error-list ul, .warning-list ul {{
            margin-left: 20px;
            margin-top: 10px;
        }}

        .metadata {{
            background: var(--color-bg);
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 30px;
        }}

        .metadata-item {{
            display: flex;
            margin: 5px 0;
        }}

        .metadata-label {{
            font-weight: 600;
            margin-right: 10px;
            min-width: 150px;
        }}

        .collapsible {{
            cursor: pointer;
            padding: 10px;
            background: #f1f1f1;
            border: none;
            text-align: left;
            width: 100%;
            outline: none;
            transition: 0.3s;
            border-radius: 5px;
            margin: 10px 0;
        }}

        .collapsible:hover {{
            background: #e1e1e1;
        }}

        .collapsible-content {{
            display: none;
            padding: 15px;
            background: white;
            border: 1px solid var(--color-border);
            border-radius: 5px;
            margin-bottom: 10px;
        }}

        .collapsible.active + .collapsible-content {{
            display: block;
        }}

        .progress-bar {{
            width: 100%;
            height: 30px;
            background: #e0e0e0;
            border-radius: 15px;
            overflow: hidden;
            margin: 20px 0;
        }}

        .progress-fill {{
            height: 100%;
            background: linear-gradient(90deg, var(--color-pass) 0%, var(--color-warning) 50%, var(--color-fail) 100%);
            transition: width 0.3s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
        }}

        .footer {{
            margin-top: 50px;
            padding-top: 20px;
            border-top: 2px solid var(--color-border);
            text-align: center;
            color: #6c757d;
            font-size: 0.9em;
        }}

        @media (max-width: 768px) {{
            .container {{
                padding: 15px;
            }}

            .summary-grid {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        {generate_header(results)}
        {generate_metadata_section(results)}
        {generate_summary_section(results)}
        {generate_validation_sections(results)}
        {generate_footer(results)}
    </div>

    <script>
        // Collapsible sections
        document.querySelectorAll('.collapsible').forEach(button => {{
            button.addEventListener('click', function() {{
                this.classList.toggle('active');
            }});
        }});
    </script>
</body>
</html>
"""

    # Write the report
    with open(output_file, 'w') as f:
        f.write(html_content)


def generate_header(results: Dict) -> str:
    """Generate the report header."""
    status = results.get('overall_status', 'UNKNOWN')
    status_class = f'status-{status.lower()}'

    return f"""
        <h1>
            xclim-timber Validation Report
            <span class="status-badge {status_class}">{status}</span>
        </h1>
        <p style="font-size: 1.1em; color: #666;">
            {results.get('message', 'Validation complete')}
        </p>
    """


def generate_metadata_section(results: Dict) -> str:
    """Generate the metadata section."""
    return f"""
        <div class="metadata">
            <div class="metadata-item">
                <span class="metadata-label">Pipeline:</span>
                <span>{results.get('pipeline', 'Unknown')}</span>
            </div>
            <div class="metadata-item">
                <span class="metadata-label">Directory:</span>
                <span>{results.get('directory', 'Unknown')}</span>
            </div>
            <div class="metadata-item">
                <span class="metadata-label">Timestamp:</span>
                <span>{results.get('timestamp', datetime.now().isoformat())}</span>
            </div>
            <div class="metadata-item">
                <span class="metadata-label">Validation Mode:</span>
                <span>{results.get('validation_mode', 'full').title()}</span>
            </div>
        </div>
    """


def generate_summary_section(results: Dict) -> str:
    """Generate the summary statistics section."""
    summary = results.get('summary', {})

    total = summary.get('total_checks', 0)
    passed = summary.get('passed', 0)
    failed = summary.get('failed', 0)
    warnings = summary.get('warnings', 0)

    # Calculate pass rate
    pass_rate = (passed / total * 100) if total > 0 else 0

    return f"""
        <h2>Summary Statistics</h2>

        <div class="progress-bar">
            <div class="progress-fill" style="width: {pass_rate}%;">
                {pass_rate:.1f}% Pass Rate
            </div>
        </div>

        <div class="summary-grid">
            <div class="summary-card">
                <div class="summary-label">Total Checks</div>
                <div class="summary-value">{total}</div>
            </div>
            <div class="summary-card pass">
                <div class="summary-label">Passed</div>
                <div class="summary-value" style="color: var(--color-pass);">{passed}</div>
            </div>
            <div class="summary-card warning">
                <div class="summary-label">Warnings</div>
                <div class="summary-value" style="color: var(--color-warning);">{warnings}</div>
            </div>
            <div class="summary-card fail">
                <div class="summary-label">Failed</div>
                <div class="summary-value" style="color: var(--color-fail);">{failed}</div>
            </div>
        </div>
    """


def generate_validation_sections(results: Dict) -> str:
    """Generate detailed validation sections."""
    sections_html = "<h2>Detailed Validation Results</h2>"

    validations = results.get('validations', {})

    for category_name, category_results in validations.items():
        # Format category name
        display_name = category_name.replace('_', ' ').title()

        # Determine status and icon
        status = category_results.get('status', 'UNKNOWN')
        status_icon = {
            'PASS': '✅',
            'WARNING': '⚠️',
            'FAIL': '❌',
            'ERROR': '🔥'
        }.get(status, '❓')

        sections_html += f"""
        <div class="validation-section">
            <div class="validation-header">
                <h3>{status_icon} {display_name}</h3>
                <span class="status-badge status-{status.lower()}">{status}</span>
            </div>
        """

        # Add category-specific content
        if 'checks' in category_results:
            sections_html += generate_checks_table(category_results['checks'])

        # Show errors if any
        if 'errors' in category_results and category_results['errors']:
            if isinstance(category_results['errors'], int):
                sections_html += f"<p>Errors: {category_results['errors']}</p>"
            else:
                sections_html += '<div class="error-list"><strong>Errors:</strong><ul>'
                for error in category_results['errors'][:10]:  # Show first 10
                    sections_html += f"<li>{error}</li>"
                sections_html += '</ul></div>'

        # Show warnings if any
        if 'warnings' in category_results and category_results['warnings']:
            if isinstance(category_results['warnings'], int):
                sections_html += f"<p>Warnings: {category_results['warnings']}</p>"
            else:
                sections_html += '<div class="warning-list"><strong>Warnings:</strong><ul>'
                for warning in category_results['warnings'][:10]:  # Show first 10
                    sections_html += f"<li>{warning}</li>"
                sections_html += '</ul></div>'

        sections_html += "</div>"

    return sections_html


def generate_checks_table(checks: Dict) -> str:
    """Generate a table for validation checks."""
    if not checks:
        return ""

    # For simple checks dict
    if all(isinstance(v, dict) and 'status' in v for v in checks.values()):
        html = """
        <table>
            <thead>
                <tr>
                    <th>Check</th>
                    <th>Status</th>
                    <th>Details</th>
                </tr>
            </thead>
            <tbody>
        """

        for check_name, check_result in list(checks.items())[:20]:  # Limit to 20 rows
            status = check_result.get('status', 'UNKNOWN')
            message = check_result.get('message', '')

            status_color = {
                'PASS': 'var(--color-pass)',
                'WARNING': 'var(--color-warning)',
                'FAIL': 'var(--color-fail)',
                'ERROR': 'var(--color-error)'
            }.get(status, '#666')

            html += f"""
                <tr>
                    <td>{check_name}</td>
                    <td style="color: {status_color}; font-weight: bold;">{status}</td>
                    <td>{message}</td>
                </tr>
            """

        html += """
            </tbody>
        </table>
        """
        return html

    # For complex nested structure
    return f"""
        <button class="collapsible">Show Detailed Checks</button>
        <div class="collapsible-content">
            <pre>{json.dumps(checks, indent=2, default=str)}</pre>
        </div>
    """


def generate_footer(results: Dict) -> str:
    """Generate the report footer."""
    return f"""
        <div class="footer">
            <p>Generated by xclim-timber Validation Suite v1.0.0</p>
            <p>Report generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    """