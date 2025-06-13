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
    'sphinx.ext.viewcode',
    'sphinx.ext.githubpages',
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
]


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
templates_path = ['_templates']
html_static_path = ['_static']
html_css_files = ['other/custom.css']
html_logo = "_static/images/logo_geoclimate.png"


# Limiter la profondeur du menu latéral aux seuls titres de niveau 1
html_theme_options = {
    'navigation_depth': 2,
    'collapse_navigation': False,
    'titles_only': True,  # Optionnel, enlève aussi les sous-éléments redondants
}

html_context = {
    "display_github": True, # Add 'Edit on GitHub' link
    "github_repo": "orbisgis/geoclimate",
    "github_version": "master",  # or "master" or any branch
    "conf_py_path": "/docs/source/",  # Path in the repo to your docs root (with trailing slash)
}


