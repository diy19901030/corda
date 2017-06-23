package net.corda.core.serialization.carpenter

import org.objectweb.asm.Type
import java.util.LinkedHashMap

open class Schema(
        val name: String,
        fields: Map<String, Class<out Any?>>,
        val superclass: Schema? = null,
        val interfaces: List<Class<*>> = emptyList())
{
    val fields = LinkedHashMap(fields)  // Fix the order up front if the user didn't.
    val descriptors = fields.map { it.key to Type.getDescriptor(it.value) }.toMap()

    fun fieldsIncludingSuperclasses(): Map<String, Class<out Any?>> = (superclass?.fieldsIncludingSuperclasses() ?: emptyMap()) + LinkedHashMap(fields)
    fun descriptorsIncludingSuperclasses(): Map<String, String> =  (superclass?.descriptorsIncludingSuperclasses() ?: emptyMap()) + LinkedHashMap(descriptors)

    val jvmName: String
        get() = name.replace(".", "/")
}

class ClassSchema(
    name: String,
    fields: Map<String, Class<out Any?>>,
    superclass: Schema? = null,
    interfaces: List<Class<*>> = emptyList()
) : Schema (name, fields, superclass, interfaces)

class InterfaceSchema(
    name: String,
    fields: Map<String, Class<out Any?>>,
    superclass: Schema? = null,
    interfaces: List<Class<*>> = emptyList()
) : Schema (name, fields, superclass, interfaces)
