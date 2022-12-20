# Import setuptools before distutils because setuptools monkey patches
# distutils:
#
# https://github.com/pypa/setuptools/commit/bd1102648109c85c782286787e4d5290ae280abe
import setuptools

import atexit
import distutils.command.build
import os
import subprocess
import tempfile

import setuptools.command.build_py
import setuptools.command.develop
import setuptools.command.install
import setuptools.command.sdist

root_dir = os.path.dirname(os.path.abspath(__file__))
frontend_dir = os.path.join(root_dir, 'frontend')
frontend_dist_dir = os.path.join(root_dir, 'beancount_import', 'frontend_dist')

with open(os.path.join(root_dir, 'README.md'),
          'r',
          newline='\n',
          encoding='utf-8') as f:
    long_description = f.read()


def _setup_temp_egg_info(cmd):
    """Use a temporary directory for the `.egg-info` directory.

  When building an sdist (source distribution) or installing, locate the
  `.egg-info` directory inside a temporary directory so that it
  doesn't litter the source directory and doesn't pick up a stale SOURCES.txt
  from a previous build.
  """
    egg_info_cmd = cmd.distribution.get_command_obj('egg_info')
    if egg_info_cmd.egg_base is None:
        tempdir = tempfile.TemporaryDirectory(dir=os.curdir)
        egg_info_cmd.egg_base = tempdir.name
        atexit.register(tempdir.cleanup)


class SdistCommand(setuptools.command.sdist.sdist):
    def run(self):
        _setup_temp_egg_info(self)
        self.run_command('bundle_frontend')
        super().run()

    def make_release_tree(self, base_dir, files):
        # Exclude .egg-info from source distribution.  These aren't actually
        # needed, and due to the use of the temporary directory in `run`, the
        # path isn't correct if it gets included.
        files = [x for x in files if '.egg-info' not in x]
        super().make_release_tree(base_dir, files)


class BuildCommand(distutils.command.build.build):
    def finalize_options(self):
        if self.build_base == 'build':
            # Use temporary directory instead, to avoid littering the source directory
            # with a `build` sub-directory.
            tempdir = tempfile.TemporaryDirectory()
            self.build_base = tempdir.name
            atexit.register(tempdir.cleanup)
        super().finalize_options()

    def run(self):
        self.run_command('bundle_frontend')
        super().run()


class InstallCommand(setuptools.command.install.install):
    def run(self):
        _setup_temp_egg_info(self)
        self.run_command('bundle_frontend')
        super().run()


class DevelopCommand(setuptools.command.develop.develop):
    def run(self):
        self.run_command('bundle_frontend')
        super().run()


class BundleFrontendCommand(setuptools.command.build_py.build_py):

    user_options = setuptools.command.build_py.build_py.user_options + [
        ('bundle-type=', None,
         'The bundle type. "min" (default) creates minified bundles,'
         ' "dev" creates non-minified files.'),
        ('skip-npm-reinstall', None,
         'Skip running `npm install` if the `node_modules` directory already exists.'),
        ('skip-rebuild', None,
         'Skip rebuilding if the `frontend_dist/app.js` file already exists.'),
    ]

    def initialize_options(self):

        self.bundle_type = 'min'
        self.skip_npm_reinstall = None
        self.skip_rebuild = None

    def finalize_options(self):

        if self.bundle_type not in ['min', 'dev']:
            raise RuntimeError('bundle-type has to be one of "min" or "dev"')

        if self.skip_npm_reinstall is None:
            self.skip_npm_reinstall = False

        if self.skip_rebuild is None:
            self.skip_rebuild = False

    def run(self):
        if self.skip_rebuild:
            bundle_path = os.path.join(frontend_dist_dir, 'app.js')
            if os.path.exists(bundle_path):
                print('Skipping rebuild of frontend bundle since %s already exists' % (bundle_path, ))
                return

        target = {"min": "build", "dev": "builddev"}

        try:
            t = target[self.bundle_type]
            node_modules_path = os.path.join(frontend_dir, 'node_modules')
            if (self.skip_npm_reinstall and os.path.exists(node_modules_path)):
                print('Skipping `npm install` since %s already exists' %
                      (node_modules_path, ))
            else:
                subprocess.call('npm i', shell=True, cwd=frontend_dir)
            res = subprocess.call('npm run %s' % t, shell=True, cwd=frontend_dir)
        except:
            raise RuntimeError('Could not run \'npm run %s\'.' % t)

        if res != 0:
            raise RuntimeError('failed to build frontend bundles')


setuptools.setup(
    name='beancount-import',
    # Use setuptools_scm to determine version from git tags
    use_scm_version={
        # It would be nice to include the commit hash in the version, but that
        # can't be done in a PEP 440-compatible way.
        'version_scheme': 'no-guess-dev',
        # Test PyPI does not support local versions.
        'local_scheme': 'no-local-version',
        'fallback_version': '0.0.0',
    },
    description='Semi-automatic importing of external data into beancount.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/jbms/beancount-import',
    author='Jeremy Maitin-Shepard',
    author_email="jeremy@jeremyms.com",
    license='GPLv2',
    packages=[
        "beancount_import",
        "beancount_import.source",
    ],
    package_data={
        'beancount_import': ['frontend_dist/*'],
    },
    python_requires='>=3.7',
    setup_requires=['setuptools_scm>=5.0.2'],
    install_requires=[
        'beancount>=2.1.3',
        'tornado',
        'numpy',
        'scipy',
        'scikit-learn~=1.2',
        'nltk',
        'python-dateutil',
        'atomicwrites>=1.3.0',
        'jsonschema',
        'watchdog',
        'typing_extensions',
    ],
    test_requirements=[
        'pytest',
        'coverage',
    ],
    cmdclass={
        'sdist': SdistCommand,
        'build': BuildCommand,
        'install': InstallCommand,
        'bundle_frontend': BundleFrontendCommand,
        'develop': DevelopCommand,
    },
)
