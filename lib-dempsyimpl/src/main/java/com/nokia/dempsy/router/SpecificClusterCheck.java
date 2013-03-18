package com.nokia.dempsy.router;

import com.nokia.dempsy.config.ClusterId;

/**
 * This class is deprecated. Please use the {@link RegExClusterCheck}. It is included
 * for backward compatibility but WILL be removed before 1.0.
 * 
 * @deprecated
 */
@Deprecated
public class SpecificClusterCheck extends RegExClusterCheck
{
   public SpecificClusterCheck(ClusterId clusterId) { super(clusterId); }
}
