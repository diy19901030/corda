package net.corda.core.serialization

import org.objectweb.asm.Type
import java.util.LinkedHashMap

/**
 * A Schema represents a desired class.
 */
class ClassCarpenterSchema(
        val name: String,
        fields: Map<String, Class<out Any?>>,
        val superclass: ClassCarpenterSchema? = null,
        val interfaces: List<Class<*>> = emptyList(),
        val isInterface: Boolean = false)
{
    val fields = LinkedHashMap(fields)  // Fix the order up front if the user didn't.
    val descriptors = fields.map { it.key to Type.getDescriptor(it.value) }.toMap()

    fun fieldsIncludingSuperclasses(): Map<String, Class<out Any?>> = (superclass?.fieldsIncludingSuperclasses() ?: emptyMap()) + LinkedHashMap(fields)
    fun descriptorsIncludingSuperclasses(): Map<String, String> =  (superclass?.descriptorsIncludingSuperclasses() ?: emptyMap()) + LinkedHashMap(descriptors)
}
