package net.dempsy.router.group.intern;

import net.dempsy.router.group.ClusterGroupInbound;

public class GroupUtils {

    public static String groupNameFromTypeId(final String typeId) {
        final int colon = typeId.indexOf(':');
        if (colon < 0)
            throw new IllegalArgumentException("The typeId for a routing strategy for " + ClusterGroupInbound.class.getPackage().getName()
                    + " must contain a group name. It should have the form \"" + ClusterGroupInbound.class.getPackage().getName()
                    + ":(groupname)\"");
        final String groupName = typeId.substring(colon + 1).trim();
        if (groupName == null || " ".equals(groupName) || groupName.length() == 0)
            throw new IllegalArgumentException("The typeId for a routing strategy for " + ClusterGroupInbound.class.getPackage().getName()
                    + " must contain a group name. It should have the form \"" + ClusterGroupInbound.class.getPackage().getName()
                    + ":(groupname)\"");
        return groupName;
    }

    public static String groupNameFromTypeIdDontThrowNoColon(final String typeId) {
        final int colon = typeId.indexOf(':');
        if (colon < 0)
            return null;
        final String groupName = typeId.substring(colon + 1).trim();
        if (groupName == null || " ".equals(groupName) || groupName.length() == 0)
            throw new IllegalArgumentException("The typeId for a routing strategy for " + ClusterGroupInbound.class.getPackage().getName()
                    + " must contain a group name. It should have the form \"" + ClusterGroupInbound.class.getPackage().getName()
                    + ":(groupname)\"");
        return groupName;
    }

}
