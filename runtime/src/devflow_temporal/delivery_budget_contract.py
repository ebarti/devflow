"""Pure finite repair ceilings shared by workflow code and stopped admission.

This module reads only frozen input data. It must not import runtime, filesystem,
provider, or custody code because Temporal workflows execute it in the sandbox.
"""

def fixed_budget_allows(spec, iteration, iterations, *, pending=False):
    """Spend original turns or the finite extension sealed by a runtime allowance."""
    maximum = spec.get('policy', {}).get('max_repairs')
    authority = spec.get('repair_allowance')
    if authority is not None:
        if (not spec.get('feature_worker') or not isinstance(authority, dict)
                or authority.get('run_id') != spec.get('run_id')
                or type(authority.get('start_iteration')) is not int
                or type(authority.get('maximum_iteration')) is not int
                or not 0 <= authority['start_iteration'] <= authority['maximum_iteration']
                or authority['maximum_iteration'] - authority['start_iteration']
                    > 100 + int(bool(authority.get('credit')))):
            return False
        maximum = authority['maximum_iteration']
    allowed = range(1, 102) if spec.get('feature_worker') else (1, 2)
    return (type(iteration) is int and iteration >= 0
            and type(iterations) is int
            and (iterations in allowed or (iterations == 0 and pending))
            and type(maximum) is int and maximum >= 0
            and iteration + iterations <= maximum)
