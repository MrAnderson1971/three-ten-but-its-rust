use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Type};

#[proc_macro_derive(FieldGetter, attributes(field_prefix))]
pub fn field_getter_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Extract the prefix from attributes
    let prefix = extract_prefix(&input.attrs).unwrap_or_else(|| "".to_string());

    // Extract fields from the struct
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("FieldGetter only works with named fields"),
        },
        _ => panic!("FieldGetter only works with structs"),
    };

    // Generate match arms for each field
    let match_arms = fields.iter().map(|field| {
        let field_name = field.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();
        let field_type = &field.ty;

        // Create the prefixed field name
        let prefixed_name = format!("{}{}", prefix, field_name_str);

        // Determine how to convert the field to Value
        let conversion = generate_conversion(field_type, quote!(self.#field_name));

        quote! {
            #prefixed_name => {
                Ok(#conversion)
            }
        }
    });

    // Generate the implementation
    let expanded = quote! {
        impl #name {
            pub fn get(&self, field_name: &str) -> Result<Value, String> {
                match field_name {
                    #(#match_arms)*
                    _ => Err(format!("Field '{}' not found. Fields must start with prefix '{}'", field_name, #prefix)),
                }
            }
        }
    };

    TokenStream::from(expanded)
}

// Extract prefix from attributes - Updated for syn 2.0
fn extract_prefix(attrs: &[syn::Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("field_prefix") {
            // Parse the attribute value
            if let Ok(value) = attr.parse_args::<syn::LitStr>() {
                return Some(value.value());
            }
            // Also try parsing as name = value
            if let Ok(expr) = attr.parse_args::<syn::Expr>() {
                if let syn::Expr::Assign(assign) = expr {
                    if let syn::Expr::Lit(syn::ExprLit {
                                              lit: syn::Lit::Str(lit_str),
                                              ..
                                          }) = &*assign.right {
                        return Some(lit_str.value());
                    }
                }
            }
        }
    }
    None
}

// Helper function to generate conversion based on type
fn generate_conversion(ty: &Type, field_access: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let ty_str = quote!(#ty).to_string();

    // Handle different types
    if ty_str.contains("String") || ty_str.contains("str") {
        quote! { Value::Str(#field_access.clone()) }
    } else if ty_str.contains("f32") {
        quote! { Value::Num(#field_access) }
    } else if ty_str.contains("f64") {
        quote! { Value::Num(#field_access as f32) }
    } else if ty_str.contains("i32") || ty_str.contains("u32") ||
        ty_str.contains("i64") || ty_str.contains("u64") ||
        ty_str.contains("usize") || ty_str.contains("isize") {
        quote! { Value::Num(#field_access as f32) }
    } else if ty_str.contains("bool") {
        quote! { Value::Str(#field_access.to_string()) }
    } else {
        // For unknown types, try to convert to string
        quote! { Value::Str(format!("{:?}", #field_access)) }
    }
}