from timeit import default_timer as timer

import click


def profile_method(method_name, method_to_profile):
    start = timer()
    result = method_to_profile()
    end = timer()
    click.echo(f"{method_name} took {end - start} seconds")
    return result
