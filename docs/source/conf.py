# -*- coding: utf-8 -*-
import datetime

import ecgtools

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.autosummary',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.extlinks',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'myst_nb',
    'sphinxext.opengraph',
    'sphinx_copybutton',
    'sphinxcontrib.autodoc_pydantic',
    'sphinx_inline_tabs',
]


autodoc_member_order = 'groupwise'

# MyST config
myst_enable_extensions = ['amsmath', 'colon_fence', 'deflist', 'html_image']
myst_url_schemes = ['http', 'https', 'mailto']

# sphinx-copybutton configurations
copybutton_prompt_text = r'>>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: '
copybutton_prompt_is_regexp = True


autodoc_pydantic_model_show_json = True
autodoc_pydantic_settings_show_json = False

jupyter_execute_notebooks = 'cache'
execution_timeout = 600
execution_allow_errors = True


# Autosummary pages will be generated by sphinx-autogen instead of sphinx-build
autosummary_generate = []
autodoc_typehints = 'none'

# Napoleon configurations

napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_use_param = False
napoleon_use_rtype = False
napoleon_preprocess_types = True


# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']


# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'ecgtools'
current_year = datetime.datetime.now().year
copyright = f'2020-{current_year}, NCAR XDev Team'
author = 'NCAR XDev Team'
# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = ecgtools.__version__.split('+')[0]
# The full version, including alpha/beta/rc tags.
release = ecgtools.__version__

exclude_patterns = ['_build', '**.ipynb_checkpoints', 'Thumbs.db', '.DS_Store']


# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'


# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'furo'
html_title = ''
html_last_updated_fmt = '%b %d, %Y'

html_context = {
    'github_user': 'NCAR',
    'github_repo': 'ecgtools',
    'github_version': 'main',
    'doc_path': 'docs',
}
html_theme_options = {}

# Output file base name for HTML help builder.
htmlhelp_basename = 'ecgtoolsdoc'


# -- Options for LaTeX output --------------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    # 'papersize': 'letterpaper',
    # The font size ('10pt', '11pt' or '12pt').
    # 'pointsize': '10pt',
    # Additional stuff for the LaTeX preamble.
    # 'preamble': '',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, documentclass [howto/manual]).
latex_documents = [
    ('index', 'ecgtools.tex', 'ecgtools Documentation', 'NCAR XDev Team', 'manual'),
]
man_pages = [('index', 'ecgtools', 'ecgtools Documentation', [author], 1)]


texinfo_documents = [
    (
        'index',
        'ecgtools',
        'ecgtools Documentation',
        author,
        'ecgtools',
        'One line description of project.',
        'Miscellaneous',
    ),
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'xarray': ('http://xarray.pydata.org/en/stable/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
}
