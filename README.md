# Glusto Libs

`glusto-libs` repo contains the Libraries/Modules necessary for automating the gluster tests.
It mainly provides python bindings for the GlusterD-2.0 APIs.
Latest Code for this repo is managed on review.gluster.org


Refer the [glusto-doc](http://glusto.readthedocs.io/en/latest/) for info
on `glusto` framework.
Issues need to be filled against the
[Github](https://github.com/gluster/glusto-libs/issues) repo.


To automate/run glusto-tests on GD2 environment we need to do following steps:
-----------------------------------------------------------------------------
-   install `glusto`
-   clone `glusto-libs` repo
-   clone `glusto-tests` repo
-   install all packages (i.e, glustolibs-gluster-gd2, glustolibs-io and glustolibs-misc libraries)


How to install glusto:
----------------------
One can use either of the three methods.

-   using pip

        # pip install --upgrade git+git://github.com/loadtheaccumulator/glusto.git

-   using git

        # git clone https://github.com/loadtheaccumulator/glusto.git
        # cd glusto
        # python setup.py install

- using ansible: install glusto, glusto-tests

        # ansible-playbook -i host.ini glusto-tests/ansible/deploy-glusto.yaml


For more info refer the [docs](http://glusto.readthedocs.io/en/latest/userguide/install.html).


How to clone glusto-tests and glusto-libs repo:
------------------------------------------------
- using git

        # git clone ssh://user-name@review.gluster.org/glusto-libs

- using git

        # git clone ssh://user-name@review.gluster.org/glusto-tests


How to install the glustolibs-gluster-gd2, glustolibs-io and glustolibs-misc libraries:
---------------------------------------------------------------------------------------
    # git clone ssh://user-name@review.gluster.org/glusto-libs
    # cd glusto-libs/glustolibs-gluster-gd2
    # python setup.py install
    # cd ../../glusto-tests/glustolibs-io
    # python setup.py install
    # cd ../../glusto-tests/glustolibs-misc
    # python setup.py install


To install glusto-tests dependencies:
-------------------------------------
`python-docx` needs to be installed when we run IO's and validates on client node.

- To install run:

	# easy_install pip
	# pip install --pre python-docx

How to run the test case:
-------------------------
-   Update the information about the servers, clients,
    servers_info, client_info, running_on_volumes, running_on_mounts
    and volume_create_force etc, which is necessary on the config
    file[config](https://github.com/gluster/glusto-tests/blob/master/tests/gluster_tests_config.yml).
    Refer the following for more info [link](http://glusto.readthedocs.io/en/latest/userguide/configurable.html).

-   glusto-tests are run using the `glusto` command available after installing
    the glusto framework. The various options to run tests as provided by
    glusto framework:

    To run PyTest tests:

        - To run all tests that are marked with tag 'bvt'.

        `# glusto -c config.yml --pytest='-v -x tests -m bvt'`

        - To run all tests that are under bvt folder.

        `# glusto -c config.yml --pytest='-v -s bvt/'`

        - To run a single test case

        `# glusto -c config.yml --pytest='-v -s -k test_demo1'`

    For more info about running these tests, refer the [docs](http://glusto.readthedocs.io/en/latest/userguide/glusto.html#options-for-running-unit-tests).


Writing tests/libraries for GD2:
--------------------------------
- `tests` directory in glusto-tests contain testcases. Testcases are written as component wise.
Testcases name and file name should should start with test_.

- `glustolibs-gluster-gd2` directory in glusto-libs contains libraries for GD2 api's.
Libraries for io's and miscelleneous are written on `glustolibs-io` and `glustolibs-misc`
respectively. These functions or libraries can be used while writting testcases.

- While writting testscase or libraries follow
        - Please follow the [PEP008 style guide](https://www.python.org/dev/peps/pep-0008/).
        - Makes sure all the pylint and pyflakes error are fixed

          For example:

              - C0326: Exactly one space required around assignment
              - C0111: Missing module docstring (missing-docstring)
              - W: 50: Too long line

          For more information on [pylint](https://docs.pylint.org/en/1.6.0/tutorial.html) and on [pyflakes](http://flake8.pycqa.org/en/latest/user/error-codes.html)
        - Optimize the code as much as possible. Eliminate the repeatative steps,
          write it has separate function to avoid repeatation.
        - Use proper python standards on returning values.
          This style guide is a list of do's and don’ts for [Python programs](http://google.github.io/styleguide/pyguide.html).
        - Add docstring to every function you write

          For example: This is an example of a module level function

              def module(param1, param2):
                  """
                  Explain what the module function does in breif

                  Args:
                      param1: The first parameter.
                      param2: The second parameter.

                  Returns:
                      The return value of the function.
                  """

        - Make sure the log messages are grammatically correct and have no spelling mistakes.
        - Comment every step of the test case/libraries, log the test step, test result, failure and success.

          For example:

              # peer status from mnode
              g.log.info("Get peer status from node %s", self.mnode)
              ret, out, err = peer_status(self.mnode)
              self.assertEqual(ret, 0, "Failed to get peer status from node %s: %s" % (self.mnode, err))
              g.log.info("Successfully got peer status from node %s:\n%s", self.mnode, out)

        - Don't not use `print` statements in test-cases/libraries because prints statements are not captured in log files.
          Use logger functions to dump messages into log file.

Logging:
--------
Log file name and Log level can be passed as argument to glusto command while
running the glusto-tests. For example:

    # glusto -c 'config.yml' -l /tmp/glustotests_bvt.log --log-level DEBUG --pytest='-v -x tests -m bvt'

One can configure log files, log levels in the testcases as well. For details
on how to use glusto framework for configuring logs in tests Refer the following [docs](http://glusto.readthedocs.io/en/latest/userguide/loggable.html).

Default log location is: `/tmp/glustomain.log`

Note: When using `glusto` via the Python Interactive Interpreter,
the default log location is `/tmp/glusto.log`.
