# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Geoclimate'
copyright = '2025, CNRS-Lab-STICC'
author = 'Erwan Bocher, Jérémy Bernard, Elisabeth Le Saux Wiederhold, François Leconte, Matthieu Gousseff, Emmanuelle Kerjouan, Gwendall Petit, Sylvain Palominos, Maxime Collombin, Emmanuel Renaud'
release = '1.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'myst_parser',
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.mathjax',
    'sphinx.ext.napoleon',
]

# Configuration for LaTeX
latex_elements = {
    'papersize': 'a4paper',
    'pointsize': '11pt',
    'preamble': '',
}



source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}

exclude_patterns = []

language = 'English'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
templates_path = ['source/_templates']
html_static_path = ['source/_static']
html_css_files = ['custom.css']


# Limiter la profondeur du menu latéral aux seuls titres de niveau 1
html_theme_options = {
    'navigation_depth': 2,
    'collapse_navigation': False,
    'titles_only': True,  # Optionnel, enlève aussi les sous-éléments redondants
}

